pub mod cli;
use crate::cli::{Cli, Commands};
use anyhow::{Context, Result};
use clap::Parser;
use futures_util::stream::StreamExt;
use serde::Deserialize;
use solana_client::{nonblocking::rpc_client, rpc_client::RpcClient};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeUpdate,
};
use yellowstone_grpc_proto::prelude::{SubscribeRequestFilterBlocks, SubscribeUpdateBlock};

static DEFAULT_GEYSER_ENDPOINT: &str = "https://printworld.shyft.to";
const CONFIG_FILE_PATH: &str = "config.yaml";

const X_TOKEN: &str = "b2b972c6-fff2-4b5c-aac9-375c6984b80e";

#[derive(Deserialize, Debug)]
struct Config {
    /// Base58-encoded sender keypair (private key)
    sender_keypair: String,
    /// Recipient public key for SOL transfer
    recipient_pubkey: String,
    /// Amount of SOL to transfer per transaction
    transfer_amount_sol: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Stream => {
            stream_block_updates().await?;
        }
    }

    Ok(())
}

async fn stream_block_updates() -> anyhow::Result<()> {
    // config from config.yaml
    let config_content = fs::read_to_string(CONFIG_FILE_PATH)
        .with_context(|| format!("Failed to read config file: {:?}", CONFIG_FILE_PATH))?;
    let config: Config =
        serde_yaml::from_str(&config_content).with_context(|| "Failed to parse config.yaml")?;

    let (tx, mut rx) = mpsc::channel::<SubscribeUpdate>(100);

    let mut client = GeyserGrpcClient::build_from_static(DEFAULT_GEYSER_ENDPOINT)
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .x_token(Some(X_TOKEN))?
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to Geyser: {:?}", e))?;

    // Set up subscription for blocks
    let mut subscribe_request = SubscribeRequest::default();

    subscribe_request.blocks.insert(
        "new_block".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: vec!["So1111111111111111111111111111111111111112".to_string()],
            include_transactions: Some(true),
            include_accounts: Some(true),
            include_entries: Some(true),
        },
    );

    tokio::spawn(async move {
        let (mut _subscribe_tx, subscribe_stream) =
            match client.subscribe_with_request(Some(subscribe_request)).await {
                Ok((tx, stream)) => {
                    println!("Subscribed to stream");
                    (tx, stream)},
                Err(e) => {
                    anyhow::anyhow!("Failed to subscribe: {:?}", e);
                    return;
                }
            };

        // process stream
        tokio::pin!(subscribe_stream);
        while let Some(message) = subscribe_stream.next().await {
            match message {
                Ok(update) => {
                    println!("Received update: {:?}", update);
                    if let Err(e) = tx.send(update).await {
                        anyhow::anyhow!("Failed to send update: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    anyhow::anyhow!("Stream error: {:?}", e);
                    continue;
                }
            }
        }
    });

    // updates
    while let Some(msg) = rx.recv().await {
        if let Some(UpdateOneof::Block(subscribe_update_block)) = msg.update_oneof {
            if let Err(e) = process_tx_update(subscribe_update_block, &config).await {
                anyhow::anyhow!("Error processing account update: {:?}", e);
                continue;
            }
        }
    }

    Ok(())
}

async fn process_tx_update(blk: SubscribeUpdateBlock, config: &Config) -> anyhow::Result<()> {
    println!("Received block update: {:?}", blk);
    let rpc_client =
        solana_client::nonblocking::rpc_client::RpcClient::new(DEFAULT_GEYSER_ENDPOINT.to_string());

    let sender_keypair = Keypair::from_base58_string(&config.sender_keypair);
    let recipient_pubkey =
        Pubkey::from_str(&config.recipient_pubkey).with_context(|| "Invalid recipient pubkey")?;
    let transfer_lamports = solana_sdk::native_token::sol_to_lamports(config.transfer_amount_sol);

    // Create SOL transfer transaction
    let instruction = system_instruction::transfer(
        &sender_keypair.pubkey(),
        &recipient_pubkey,
        transfer_lamports,
    );

    // Get recent blockhash
    let recent_blockhash: solana_sdk::hash::Hash =
        solana_sdk::hash::Hash::from_str(blk.blockhash.as_str())
            .with_context(|| "Failed to parse blockhash")?;

    // Build transaction
    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&sender_keypair.pubkey()),
        &[&sender_keypair],
        recent_blockhash,
    );

    let signature = rpc_client
        .send_and_confirm_transaction(&transaction)
        .await
        .with_context(|| "Failed to send and confirm transaction")?;

    println!("Transaction sent with signature: {}", signature);

    Ok(())
}
