# **Instrument_spot: Spot Matching Engine**

## Overview

**Instrument**  is a Rust-based spot matching engine specifically designed for cryptocurrency and stock spot market. It employs a Central Limit Order Book (CLOB) model with a First-In-First-Out (FIFO) algorithm for order matching, supporting advanced order types and conditions. The project provides a REST API with WebSocket support, built using the Actix framework, enabling real-time interaction with market data and order processing. It is highly configurable, allowing multiple instances of the matching engine to run with distinct market configurations.

The system utilizes MessagePack for communication in software and High-Frequency Trading (HFT) environments, ensuring efficient data serialization. For web and browser-based interactions, JSON is used for compatibility. The project supports complex order types, such as iceberg orders, and multiple "time-in-force" conditions, including GTC (Good Till Cancelled), GTD (Good Till Day), FOK (Fill or Kill), and IOC (Immediate or Cancel).

## Ownership and Broker Integration

The Instrument (ex: USDT_MGA) is owned by the exchange, and all traders must be subscribed through a broker. Brokers are responsible for forwarding traders' orders to the exchange's Instrument after conducting their own risk management checks.

- **HMAC Key Validation**: Each broker must have a unique HMAC key to communicate securely with the exchange's Instrument. This key is used to validate and authenticate all orders sent to the exchange.
- **Clearing and Trader Information**: Brokers can fetch clearing data and trader information directly from the central MongoDB database generated by the Instrument. This database contains the necessary trading and market data required for brokers to manage their traders effectively.
- **MessagePack Serialization**: Communication between brokers and the exchange must use the MessagePack serialization format. This ensures efficient, compact, and fast data transfer between the broker's systems and the exchange's Instrument.

## Features

- **REST API Endpoints**: Provides endpoints for managing orders and retrieving market data.
- **WebSocket Support**: Real-time data streaming for market events to connected clients.
- **MongoDB Integration**: Stores trading information and market data for later analysis and retrieval.
- **Configurable Market Instances**: Allows multiple instruments to run in parallel using different market configurations defined in JSON files.
- **Scalable Architecture**: Can be deployed on multiple servers with unified server names using Nginx.
- **Broker-Based Order Processing**: Traders' orders are processed through brokers, who must pass risk management checks before submitting orders to the exchange.

## Configuration

### `marketcg.json`

To run different market instances, configure the `marketcg.json` file at the root of the project. This file defines the market parameters, including exchange name, contract specifications, and tick sizes.

Example configuration:
```json
{
  "exchange": "Your_exchange_name",
  "market_name": "your_instrument_name",
  "asset_a_name":"the_base",
  "asset_b_name":"the_quote"
}
```
### Environment Variables

The server URL and other necessary configurations can be defined in the `.env` file. Each instrument will have a different server URL, but they can be unified under a single server name using Nginx.

## Project Structure

### REST API Endpoints

The following routes are available for order management and market data retrieval. All routes are dynamically built using the `market_name` specified in the `marketcg.json` file.

#### Order Management

- `/order/limit_order/{market_name}`: Handle limit orders
- `/order/iceberg_order/{market_name}`: Handle iceberg orders
- `/order/market_order/{market_name}`: Handle market orders
- `/order/stop_order/{market_name}`: Handle stop orders
- `/order/stoplimit_order/{market_name}`: Handle stop-limit orders
- `/order/modify_order/{market_name}`: Modify existing orders
- `/order/modify_iceberg_order/{market_name}`: Modify iceberg orders
- `/order/delete_order/{market_name}`: Delete orders
- `/order/delete_iceberg_order/{market_name}`: Delete iceberg orders
- `/order/save/{market_name}`: Save orders to the database

#### Historical Data

- `/history_last/{market_name}`: Retrieve last traded prices
- `/history_bbo/{market_name}`: Retrieve best bid and offer
- `/history_tns/{market_name}`: Retrieve trade and sales data
- `/history_mbpevent/{market_name}`: Retrieve market-by-price events
- `/history_volume/{market_name}`: Retrieve volume data
- `/full_ob/{market_name}`: Retrieve the full order book

### WebSocket Routes

WebSocket routes allow real-time data streaming for connected clients:

- `/ws/last_rt/{market_name}`: Real-time last traded prices
- `/ws/mbp_event_rt/{market_name}`: Real-time market-by-price events
- `/ws/best_bid_offer_rt/{market_name}`: Real-time best bid and offer
- `/ws/volume_rt/{market_name}`: Real-time volume updates
- `/ws/time_sale_rt/{market_name}`: Real-time time and sales data

## Usage

### Starting the Server

Ensure you have the required environment variables and market configurations set up in the `.env` file, `marketcg.json` and others market configuration file before running the server.

```bash
cargo run --release
```

### Example Nginx Configuration

To unify multiple instrument instances under a single server name, you can use an Nginx configuration like this:

```nginx
server {
    listen 80;
    server_name example.com;

    location /USDT_MGA {
        proxy_pass http://127.0.0.1:8081;
    }

    location /Orinasako_action_mga {
        proxy_pass http://127.0.0.1:8082;
    }
}
```
## Dependencies

This project uses the following main dependencies:

- **Actix-Web**: For building the REST API and WebSocket server.
- **MongoDB**: For database operations to store market data and trading information.
- **shared_struct_spot**: For global data structures shared across the application.
- **MessagePack**: For efficient serialization and deserialization of data between brokers and the exchange.

## Related Repositories

- [**shared_struct_spot**](https://github.com/Andry-RALAMBOMANANTSOA/shared_structs_spot): Contains global structures that are utilized by the Instrument matching engine.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any bugs, feature requests, or improvements.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.

## Improvement

The user's asset balance may be encrypted with AES. This matching engine should include Encryption and Decryption. You also may refactor the code in the main.rs because there are a lot repetition that can be unified in some function.

## Contact

For any questions or support, please open an issue on this repository or send me email to ralambomanantsoaandry@gmail.com.

## Soli Deo Gloria
