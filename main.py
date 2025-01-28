import asyncio
import base58
import grpc
import logging
import json
from typing import Iterator

import generated.geyser_pb2 as geyser_pb2
import generated.geyser_pb2_grpc as geyser_pb2_grpc

logger = logging.getLogger(__name__)

class TokenMonitor:
    def __init__(self, config_path: str) -> None:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)

        self.endpoint = config['rpc_url'].replace('http://', '').replace('https://', '')
        self.token = config['auth_token']
        self.TOKEN_ADDRESSES = config['token_addresses']
        self.COMMITMENT_LEVEL = geyser_pb2.CommitmentLevel.CONFIRMED

        self.channel = self._create_secure_channel()
        self.stub = geyser_pb2_grpc.GeyserStub(self.channel)

    def _create_secure_channel(self) -> grpc.Channel:
        auth = grpc.metadata_call_credentials(
            lambda context, callback: callback((("x-token", self.token),), None)
        )
        ssl_creds = grpc.ssl_channel_credentials()
        combined_creds = grpc.composite_channel_credentials(ssl_creds, auth)
        return grpc.secure_channel(self.endpoint, credentials=combined_creds)

    def request_iterator(self) -> Iterator[geyser_pb2.SubscribeRequest]:
        request = geyser_pb2.SubscribeRequest()
        for token_address in self.TOKEN_ADDRESSES.values():
            request.transactions["tokenTransfers"].account_include.extend([token_address])
        request.commitment = self.COMMITMENT_LEVEL
        yield request

    def handle_update(self, update: geyser_pb2.SubscribeUpdate) -> None:
        logger.info(f"Received update: {update}")

        if not self._is_valid_token_transaction(update):
            return

        tx_info = update.transaction.transaction
        message = tx_info.transaction.message

        print(f"\nTransaction Signature: {base58.b58encode(bytes(tx_info.signature)).decode()}")
        print(f"Slot: {update.transaction.slot}")

        print("Account Keys:")
        for key in message.account_keys:
            print(f"  - {base58.b58encode(bytes(key)).decode()}")

        print("Instructions:")
        for instruction in message.instructions:
            program_id = base58.b58encode(bytes(message.account_keys[instruction.program_id_index])).decode()
            print(f"  Program ID: {program_id}")
            
            accounts = []
            for acc in instruction.accounts:
                if acc < len(message.account_keys):
                    accounts.append(base58.b58encode(bytes(message.account_keys[acc])).decode())
            print(f"  Accounts: {accounts}")

            print(f"  Data: {base58.b58encode(instruction.data).decode()}")

        if hasattr(update.transaction, 'meta'):
            meta = update.transaction.meta

            print(f"Transaction Fee: {meta.fee}")

            print("Pre Balances:")
            for balance in meta.pre_balances:
                print(f"  {balance}")
            print("Post Balances:")
            for balance in meta.post_balances:
                print(f"  {balance}")

            print("Pre Token Balances:")
            for token_balance in meta.pre_token_balances:
                print(f"  Account Index: {token_balance.account_index}, Mint: {token_balance.mint}, Amount: {token_balance.ui_token_amount.amount}")
            print("Post Token Balances:")
            for token_balance in meta.post_token_balances:
                print(f"  Account Index: {token_balance.account_index}, Mint: {token_balance.mint}, Amount: {token_balance.ui_token_amount.amount}")

            print("Log Messages:")
            for log in meta.log_messages:
                print(f"  {log}")

            print("Rewards:")
            for reward in meta.rewards:
                print(f"  Pubkey: {reward.pubkey}, Lamports: {reward.lamports}, Post Balance: {reward.post_balance}, Reward Type: {reward.reward_type}")

            print(f"Compute Units Consumed: {meta.compute_units_consumed}")

        print("\n")

    def _is_valid_token_transaction(self, update: geyser_pb2.SubscribeUpdate) -> bool:
        return (
            hasattr(update, 'transaction') 
            and update.transaction 
            and "tokenTransfers" in update.filters
            and update.transaction.transaction
            and update.transaction.transaction.transaction
            and update.transaction.transaction.transaction.message
        )

    async def start_monitoring(self) -> None:
        try:
            responses = self.stub.Subscribe(self.request_iterator())
            for response in responses:
                self.handle_update(response)
        except grpc.RpcError as e:
            logger.error(f"gRPC error occurred: {e}")
            raise
        finally:
            self.channel.close()

def main():
    logging.basicConfig(level=logging.INFO)
    monitor = TokenMonitor(config_path='config.json')
    try:
        asyncio.run(monitor.start_monitoring())
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    main()