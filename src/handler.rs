use actix_web::{rt::time, web,HttpRequest, HttpResponse, Responder};
use serde_json::json;
use rmp_serde::to_vec_named;
use mongodb::{Database,bson::doc,bson,Collection};
use tokio::sync::mpsc;
use shared_structs_spot::*;
use crate::env_coll_decl::CollConfig;
use crate::function::*;
use std::sync::mpsc as sync_mpsc;
use chrono::{DateTime, Utc, SecondsFormat,Duration,NaiveDateTime,Local,TimeZone};
use std::sync::{Arc,Mutex};
use std::{default, env};
use crate::dedic_structs::*;

pub async fn limit_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<LimitOrder>, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = match req.headers().get("X-HMAC-SIGNATURE") {
            Some(hmac_value) => match hmac_value.to_str() {
                Ok(hmac_str) => hmac_str,
                Err(_) => return HttpResponse::BadRequest().body("Invalid HMAC signature format"),
            },
            None => return HttpResponse::BadRequest().body("HMAC signature missing"),
        };
        let limit_order = data.clone();
 
                // Deserialize the body to extract the broker identifier
                
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&limit_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if limit_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            // Retrieve market specifications for the limit order's market
                    
                            if let Err(err) = validate_limit_order(&limit_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&limit_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_limit_specs(&limit_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                          // Match the limit order side
                          match limit_order.order_side {
                            OrderSide::Buy => {
                                let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, limit_order.trader_identifier).await;
                                let balance_s = match balance {
                                Ok(user) => user, // Extract the user if the result is Ok
                                Err(_) => {
                                    return HttpResponse::InternalServerError().body("Error during balance fetching");
                                    }
                                };
                                 // Fetch all pending Buy orders for the trader
                                 let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                 fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,limit_order.trader_identifier,"Buy",)
                                 .await;
                                let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,limit_order.trader_identifier,"Buy",)
                                .await;
                            let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,limit_order.trader_identifier,"Buy",)
                                .await;
                            let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,limit_order.trader_identifier,"Buy",)
                                .await;
                             
                             let pending_orders = match pending_orders_result {
                                 Ok(orders) => orders,
                                 Err(_) => {
                                     return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                 }
                             };
                             let pending_sorders = match pending_sorders_result {
                                Ok(orders) => orders,
                                Err(_) => {
                                    return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                }
                            };
                            let pending_slorders = match pending_slorders_result {
                                Ok(orders) => orders,
                                Err(_) => {
                                    return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                }
                            };
                            let pending_iceberg = match pending_iceberg_result {
                                Ok(orders) => orders,
                                Err(_) => {
                                    return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                }
                            };
                             
                             // Calculate the balance requirement for Buy orders
                             let mut balance_req = limit_order.order_quantity * limit_order.price as f32;
                             
                             // If there are pending orders, sum their (order_quantity * price)
                             if !pending_orders.is_empty() {
                                 for order in pending_orders {
                                     balance_req += order.order_quantity * order.price as f32;
                                 }
                             }
                             if !pending_sorders.is_empty() {
                                for order in pending_sorders {
                                    balance_req += order.order_quantity * order.trigger_price as f32;
                                }
                            }
                            if !pending_slorders.is_empty() {
                                for order in pending_slorders {
                                    balance_req += order.order_quantity * order.price as f32;
                                }
                            }
                            if !pending_iceberg.is_empty() {
                                for order in pending_iceberg {
                                    balance_req += order.resting_quantity * order.price as f32;
                                }
                            }
                                
                                if balance_s.asset_balance < balance_req {
                                    return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                }
                            }
                            OrderSide::Sell => {
                                let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, limit_order.trader_identifier).await;
                                let balance_s = match balance {
                                Ok(user) => user, // Extract the user if the result is Ok
                                Err(_) => {
                                    return HttpResponse::InternalServerError().body("Error during balance fetching");
                                    }
                                };
                                 // Fetch all pending Sell orders for the trader
                                 let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                 fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,limit_order.trader_identifier,"Sell",)
                                 .await;
                                 let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                 fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,limit_order.trader_identifier,"Sell",)
                                 .await;
                             let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                 fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,limit_order.trader_identifier,"Sell",)
                                 .await;
                             let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                 fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,limit_order.trader_identifier,"Sell",)
                                 .await;
                              
                              let pending_orders = match pending_orders_result {
                                  Ok(orders) => orders,
                                  Err(_) => {
                                      return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                  }
                              };
                              let pending_sorders = match pending_sorders_result {
                                 Ok(orders) => orders,
                                 Err(_) => {
                                     return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                 }
                             };
                             let pending_slorders = match pending_slorders_result {
                                 Ok(orders) => orders,
                                 Err(_) => {
                                     return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                 }
                             };
                             let pending_iceberg = match pending_iceberg_result {
                                 Ok(orders) => orders,
                                 Err(_) => {
                                     return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                 }
                             };
                             
                             // Calculate the balance requirement for Sell orders
                             let mut balance_req = limit_order.order_quantity;
                             
                             // If there are pending orders, sum their order_quantity
                             if !pending_orders.is_empty() {
                                 for order in pending_orders {
                                     balance_req += order.order_quantity;
                                 }
                             }
                             if !pending_sorders.is_empty() {
                                for order in pending_sorders {
                                    balance_req += order.order_quantity;
                                }
                            }
                            if !pending_slorders.is_empty() {
                                for order in pending_slorders {
                                    balance_req += order.order_quantity;
                                }
                            }
                            if !pending_iceberg.is_empty() {
                                for order in pending_iceberg {
                                    balance_req += order.resting_quantity;
                                }
                            }
                             
                                if balance_s.asset_balance < balance_req {
                                    return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                }
                              
                            }
                            OrderSide::Unspecified => {
                                return HttpResponse::BadRequest().body("Order side is unspecified");
                            }
                        }
                            let json_body = match serde_json::to_string(&data) {
                                Ok(body) => body,
                                Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                            };
                            match verify_hmac_json(&json_body, received_hmac, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::LimitOrder(data.into_inner())) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("Order entered to market.");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    
                    
          
}

pub async fn market_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<MarketOrder>, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,bbo:web::Data<Arc<Mutex<BBO>>>) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
        let market_order = data.clone();
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
          
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&market_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if market_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_market_order(&market_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&market_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_market_specs(&market_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                           
                            let calc_price: Option<i32>;
                            match bbo.lock() {
                                Ok(bbo_data) => {
                                    // Successfully locked the Mutex, now access the fields of BBO
                                    calc_price = match market_order.order_side {
                                        OrderSide::Buy => bbo_data.ask_price,
                                        OrderSide::Sell => bbo_data.bid_price,
                                        _ => None, // Handle unexpected order side by setting calc_price to None
                                    };
                                }
                                Err(_) => {
                                    // Handle the error if the lock fails
                                    return HttpResponse::InternalServerError().body("Failed to lock BBO data");
                                }
                            }
                            if let Some(price) = calc_price {

                                match market_order.order_side {
                                    OrderSide::Buy => {
                                        let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, market_order.trader_identifier).await;
                                        let balance_s = match balance {
                                        Ok(user) => user, // Extract the user if the result is Ok
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error during balance fetching");
                                            }
                                        };
                                         // Fetch all pending Buy orders for the trader
                                         let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,market_order.trader_identifier,"Buy",)
                                         .await;
                                        let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,market_order.trader_identifier,"Buy",)
                                        .await;
                                    let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,market_order.trader_identifier,"Buy",)
                                        .await;
                                    let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,market_order.trader_identifier,"Buy",)
                                        .await;
                                     
                                     let pending_orders = match pending_orders_result {
                                         Ok(orders) => orders,
                                         Err(_) => {
                                             return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                         }
                                     };
                                     let pending_sorders = match pending_sorders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                        }
                                    };
                                    let pending_slorders = match pending_slorders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                        }
                                    };
                                    let pending_iceberg = match pending_iceberg_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                        }
                                    };
                                     
                                     // Calculate the balance requirement for Buy orders
                                     let mut balance_req = market_order.order_quantity * price as f32;
                                     
                                     // If there are pending orders, sum their (order_quantity * price)
                                     if !pending_orders.is_empty() {
                                         for order in pending_orders {
                                             balance_req += order.order_quantity * order.price as f32;
                                         }
                                     }
                                     if !pending_sorders.is_empty() {
                                        for order in pending_sorders {
                                            balance_req += order.order_quantity * order.trigger_price as f32;
                                        }
                                    }
                                    if !pending_slorders.is_empty() {
                                        for order in pending_slorders {
                                            balance_req += order.order_quantity * order.price as f32;
                                        }
                                    }
                                    if !pending_iceberg.is_empty() {
                                        for order in pending_iceberg {
                                            balance_req += order.resting_quantity * order.price as f32;
                                        }
                                    }
                                        
                                        if balance_s.asset_balance < balance_req {
                                            return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                        }
                                    }
                                    OrderSide::Sell => {
                                        let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, market_order.trader_identifier).await;
                                        let balance_s = match balance {
                                        Ok(user) => user, // Extract the user if the result is Ok
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error during balance fetching");
                                            }
                                        };
                                         // Fetch all pending Sell orders for the trader
                                         let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,market_order.trader_identifier,"Sell",)
                                         .await;
                                         let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,market_order.trader_identifier,"Sell",)
                                         .await;
                                     let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,market_order.trader_identifier,"Sell",)
                                         .await;
                                     let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,market_order.trader_identifier,"Sell",)
                                         .await;
                                      
                                      let pending_orders = match pending_orders_result {
                                          Ok(orders) => orders,
                                          Err(_) => {
                                              return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                          }
                                      };
                                      let pending_sorders = match pending_sorders_result {
                                         Ok(orders) => orders,
                                         Err(_) => {
                                             return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                         }
                                     };
                                     let pending_slorders = match pending_slorders_result {
                                         Ok(orders) => orders,
                                         Err(_) => {
                                             return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                         }
                                     };
                                     let pending_iceberg = match pending_iceberg_result {
                                         Ok(orders) => orders,
                                         Err(_) => {
                                             return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                         }
                                     };
                                     
                                     // Calculate the balance requirement for Sell orders
                                     let mut balance_req = market_order.order_quantity;
                                     
                                     // If there are pending orders, sum their order_quantity
                                     if !pending_orders.is_empty() {
                                         for order in pending_orders {
                                             balance_req += order.order_quantity;
                                         }
                                     }
                                     if !pending_sorders.is_empty() {
                                        for order in pending_sorders {
                                            balance_req += order.order_quantity;
                                        }
                                    }
                                    if !pending_slorders.is_empty() {
                                        for order in pending_slorders {
                                            balance_req += order.order_quantity;
                                        }
                                    }
                                    if !pending_iceberg.is_empty() {
                                        for order in pending_iceberg {
                                            balance_req += order.resting_quantity;
                                        }
                                    }
                                     
                                        if balance_s.asset_balance < balance_req {
                                            return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                        }
                                      
                                    }
                                    OrderSide::Unspecified => {
                                        return HttpResponse::BadRequest().body("Order side is unspecified");
                                    }
                                }
                               
                            } else {
                                // Handle the case where the price is None (i.e., no valid ask_price or bid_price found)
                                return HttpResponse::BadRequest().body("Invalid price: ask or bid price is not available.");
                            }
                            let json_body = match serde_json::to_string(&data) {
                                Ok(body) => body,
                                Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                            };
                            match verify_hmac_json(&json_body, hmac_str, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::MarketOrder(data.into_inner())) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                  
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn stop_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<StopOrder>,tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
        let stop_order = data.clone();
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
          
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&stop_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if stop_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_stop_order(&stop_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&stop_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_stop_specs(&stop_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            match stop_order.order_side {
                                OrderSide::Buy => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, stop_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Buy orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,stop_order.trader_identifier,"Buy",)
                                     .await;
                                    let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,stop_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,stop_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,stop_order.trader_identifier,"Buy",)
                                    .await;
                                 
                                 let pending_orders = match pending_orders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                     }
                                 };
                                 let pending_sorders = match pending_sorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                    }
                                };
                                let pending_slorders = match pending_slorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                    }
                                };
                                let pending_iceberg = match pending_iceberg_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                    }
                                };
                                 
                                 // Calculate the balance requirement for Buy orders
                                 let mut balance_req = stop_order.order_quantity * stop_order.trigger_price as f32;
                                 
                                 // If there are pending orders, sum their (order_quantity * price)
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity * order.price as f32;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity * order.trigger_price as f32;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity * order.price as f32;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity * order.price as f32;
                                    }
                                }
                                    
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                }
                                OrderSide::Sell => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, stop_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Sell orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,stop_order.trader_identifier,"Sell",)
                                     .await;
                                     let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,stop_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,stop_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,stop_order.trader_identifier,"Sell",)
                                     .await;
                                  
                                  let pending_orders = match pending_orders_result {
                                      Ok(orders) => orders,
                                      Err(_) => {
                                          return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                      }
                                  };
                                  let pending_sorders = match pending_sorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                     }
                                 };
                                 let pending_slorders = match pending_slorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                     }
                                 };
                                 let pending_iceberg = match pending_iceberg_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                     }
                                 };
                                 
                                 // Calculate the balance requirement for Sell orders
                                 let mut balance_req = stop_order.order_quantity;
                                 
                                 // If there are pending orders, sum their order_quantity
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity;
                                    }
                                }
                                 
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                  
                                }
                                OrderSide::Unspecified => {
                                    return HttpResponse::BadRequest().body("Order side is unspecified");
                                }
                            }
                            let json_body = match serde_json::to_string(&data) {
                                Ok(body) => body,
                                Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                            };
                            match verify_hmac_json(&json_body, hmac_str, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::StopOrder(data.into_inner())) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn stoplimit_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<StopLimitOrder>, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
        let stoplimit_order = data.clone();
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
             
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&stoplimit_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if stoplimit_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_stoplimit_order(&stoplimit_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&stoplimit_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_stoplimit_specs(&stoplimit_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            match stoplimit_order.order_side {
                                OrderSide::Buy => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, stoplimit_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Buy orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,stoplimit_order.trader_identifier,"Buy",)
                                     .await;
                                    let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,stoplimit_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,stoplimit_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,stoplimit_order.trader_identifier,"Buy",)
                                    .await;
                                 
                                 let pending_orders = match pending_orders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                     }
                                 };
                                 let pending_sorders = match pending_sorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                    }
                                };
                                let pending_slorders = match pending_slorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                    }
                                };
                                let pending_iceberg = match pending_iceberg_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                    }
                                };
                                 
                                 // Calculate the balance requirement for Buy orders
                                 let mut balance_req = stoplimit_order.order_quantity * stoplimit_order.price as f32;
                                 
                                 // If there are pending orders, sum their (order_quantity * price)
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity * order.price as f32;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity * order.trigger_price as f32;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity * order.price as f32;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity * order.price as f32;
                                    }
                                }
                                    
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                }
                                OrderSide::Sell => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, stoplimit_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Sell orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,stoplimit_order.trader_identifier,"Sell",)
                                     .await;
                                     let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,stoplimit_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,stoplimit_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,stoplimit_order.trader_identifier,"Sell",)
                                     .await;
                                  
                                  let pending_orders = match pending_orders_result {
                                      Ok(orders) => orders,
                                      Err(_) => {
                                          return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                      }
                                  };
                                  let pending_sorders = match pending_sorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                     }
                                 };
                                 let pending_slorders = match pending_slorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                     }
                                 };
                                 let pending_iceberg = match pending_iceberg_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                     }
                                 };
                                 
                                 // Calculate the balance requirement for Sell orders
                                 let mut balance_req = stoplimit_order.order_quantity;
                                 
                                 // If there are pending orders, sum their order_quantity
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity;
                                    }
                                }
                                 
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                  
                                }
                                OrderSide::Unspecified => {
                                    return HttpResponse::BadRequest().body("Order side is unspecified");
                                }
                            }
                            let json_body = match serde_json::to_string(&data) {
                                Ok(body) => body,
                                Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                            };
                            match verify_hmac_json(&json_body, hmac_str, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::StopLimitOrder(data.into_inner())) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}
pub async fn iceberg_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<IcebergOrder>, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
        let iceberg_order = data.clone();
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
              
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&iceberg_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if iceberg_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            // Retrieve market specifications for the limit order's market
                           
                            if let Err(err) = validate_iceberg_order(&iceberg_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&iceberg_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_iceberg_specs(&iceberg_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                           
                            match iceberg_order.order_side {
                                OrderSide::Buy => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, iceberg_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Buy orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,iceberg_order.trader_identifier,"Buy",)
                                     .await;
                                    let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,iceberg_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,iceberg_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,iceberg_order.trader_identifier,"Buy",)
                                    .await;
                                 
                                 let pending_orders = match pending_orders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                     }
                                 };
                                 let pending_sorders = match pending_sorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                    }
                                };
                                let pending_slorders = match pending_slorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                    }
                                };
                                let pending_iceberg = match pending_iceberg_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                    }
                                };
                                 
                                 // Calculate the balance requirement for Buy orders
                                 let mut balance_req = iceberg_order.total_quantity * iceberg_order.price as f32;
                                 
                                 // If there are pending orders, sum their (order_quantity * price)
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity * order.price as f32;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity * order.trigger_price as f32;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity * order.price as f32;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity * order.price as f32;
                                    }
                                }
                                    
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                }
                                OrderSide::Sell => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, iceberg_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Sell orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,iceberg_order.trader_identifier,"Sell",)
                                     .await;
                                     let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,iceberg_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,iceberg_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,iceberg_order.trader_identifier,"Sell",)
                                     .await;
                                  
                                  let pending_orders = match pending_orders_result {
                                      Ok(orders) => orders,
                                      Err(_) => {
                                          return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                      }
                                  };
                                  let pending_sorders = match pending_sorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                     }
                                 };
                                 let pending_slorders = match pending_slorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                     }
                                 };
                                 let pending_iceberg = match pending_iceberg_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                     }
                                 };
                                 
                                 // Calculate the balance requirement for Sell orders
                                 let mut balance_req = iceberg_order.total_quantity;
                                 
                                 // If there are pending orders, sum their order_quantity
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity;
                                    }
                                }
                                 
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                  
                                }
                                OrderSide::Unspecified => {
                                    return HttpResponse::BadRequest().body("Order side is unspecified");
                                }
                            }   
                            
                                let json_body = match serde_json::to_string(&data) {
                                    Ok(body) => body,
                                    Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                                };
                                match verify_hmac_json(&json_body, hmac_str, hmac_key) {
                                    Ok(true) => {
    
                                        if let Err(_) = tx.send(Structs::IcebergOrder(data.into_inner())) {
                                            return HttpResponse::InternalServerError().body("Failed to send order");
                                        }
                                        return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                    }
                                    Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                    Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                                }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn modify_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<ModifyOrder>, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
        let modify_order = data.clone();
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
               
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&modify_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if modify_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_modify_order(&modify_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&modify_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_modify_specs(&modify_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            let a_order: Result<TraderOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_order, modify_order.order_identifier).await;

                            if let Ok(a_order_s) = a_order {
                                if modify_order.trader_identifier != a_order_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                                if modify_order.new_quantity > a_order_s.order_quantity {
                                match a_order_s.order_side {
                                    OrderSide::Buy => {
                                        let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name,modify_order.trader_identifier).await;
                                        let balance_s = match balance {
                                        Ok(user) => user, // Extract the user if the result is Ok
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error during balance fetching");
                                            }
                                        };
                                        let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Buy",)
                                        .await;
                                       let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                       fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Buy",)
                                       .await;
                                   let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                       fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Buy",)
                                       .await;
                                   let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                       fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Buy",)
                                       .await;
                                    
                                    let pending_orders = match pending_orders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                        }
                                    };
                                    let pending_sorders = match pending_sorders_result {
                                       Ok(orders) => orders,
                                       Err(_) => {
                                           return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                       }
                                   };
                                   let pending_slorders = match pending_slorders_result {
                                       Ok(orders) => orders,
                                       Err(_) => {
                                           return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                       }
                                   };
                                   let pending_iceberg = match pending_iceberg_result {
                                       Ok(orders) => orders,
                                       Err(_) => {
                                           return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                       }
                                   };
                                        let mut balance_req = modify_order.new_quantity*a_order_s.price as f32;

                                        // If there are pending orders, sum their (order_quantity * price)
                                        if !pending_orders.is_empty() {
                                            for order in pending_orders {
                                                balance_req += order.order_quantity * order.price as f32;
                                            }
                                        }
                                        if !pending_sorders.is_empty() {
                                        for order in pending_sorders {
                                            balance_req += order.order_quantity * order.trigger_price as f32;
                                        }
                                    }
                                    if !pending_slorders.is_empty() {
                                        for order in pending_slorders {
                                            balance_req += order.order_quantity * order.price as f32;
                                        }
                                    }
                                    if !pending_iceberg.is_empty() {
                                        for order in pending_iceberg {
                                            balance_req += order.resting_quantity * order.price as f32;
                                        }
                                    }
                                        
                                        if balance_s.asset_balance < balance_req {
                                            return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                        }
                                    }
                                    OrderSide::Sell => {
                                        let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, modify_order.trader_identifier).await;
                                        let balance_s = match balance {
                                        Ok(user) => user, // Extract the user if the result is Ok
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error during balance fetching");
                                            }
                                        };
                                        let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Sell",)
                                        .await;
                                        let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Sell",)
                                        .await;
                                    let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Sell",)
                                        .await;
                                    let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Sell",)
                                        .await;
                                    
                                    let pending_orders = match pending_orders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                        }
                                    };
                                    let pending_sorders = match pending_sorders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                        }
                                    };
                                    let pending_slorders = match pending_slorders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                        }
                                    };
                                    let pending_iceberg = match pending_iceberg_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                        }
                                    };
                                        let mut balance_req = modify_order.new_quantity;

                                        if !pending_orders.is_empty() {
                                            for order in pending_orders {
                                                balance_req += order.order_quantity;
                                            }
                                        }
                                        if !pending_sorders.is_empty() {
                                           for order in pending_sorders {
                                               balance_req += order.order_quantity;
                                           }
                                       }
                                       if !pending_slorders.is_empty() {
                                           for order in pending_slorders {
                                               balance_req += order.order_quantity;
                                           }
                                       }
                                       if !pending_iceberg.is_empty() {
                                           for order in pending_iceberg {
                                               balance_req += order.resting_quantity;
                                           }
                                       }
    
                                        if balance_s.asset_balance < balance_req {
                                            return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                        }
                                      
                                    }
                                    OrderSide::Unspecified => {
                                        return HttpResponse::BadRequest().body("Order side is unspecified");
                                    }
                                }
                            }
                               
                            } else { //Tsisy ao @ limit order

                                let a_sorder: Result<TraderStopOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_sorder, modify_order.order_identifier).await;

                            if let Ok(a_sorder_s) = a_sorder {
                                if modify_order.trader_identifier != a_sorder_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                                if modify_order.new_quantity > a_sorder_s.order_quantity {
                                    match a_sorder_s.order_side {
                                        OrderSide::Buy => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name,modify_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Buy",)
                                            .await;
                                           let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Buy",)
                                           .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                           }
                                       };
                                       let pending_slorders = match pending_slorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                           }
                                       };
                                       let pending_iceberg = match pending_iceberg_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                           }
                                       };
                                            let mut balance_req = modify_order.new_quantity*a_sorder_s.trigger_price as f32;
    
                                            // If there are pending orders, sum their (order_quantity * price)
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity * order.price as f32;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                            for order in pending_sorders {
                                                balance_req += order.order_quantity * order.trigger_price as f32;
                                            }
                                        }
                                        if !pending_slorders.is_empty() {
                                            for order in pending_slorders {
                                                balance_req += order.order_quantity * order.price as f32;
                                            }
                                        }
                                        if !pending_iceberg.is_empty() {
                                            for order in pending_iceberg {
                                                balance_req += order.resting_quantity * order.price as f32;
                                            }
                                        }
                                            
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                        }
                                        OrderSide::Sell => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, modify_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Sell",)
                                            .await;
                                            let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                            }
                                        };
                                        let pending_slorders = match pending_slorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                            }
                                        };
                                        let pending_iceberg = match pending_iceberg_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                            }
                                        };
                                            let mut balance_req = modify_order.new_quantity;
    
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                               for order in pending_sorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_slorders.is_empty() {
                                               for order in pending_slorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_iceberg.is_empty() {
                                               for order in pending_iceberg {
                                                   balance_req += order.resting_quantity;
                                               }
                                           }
        
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                          
                                        }
                                        OrderSide::Unspecified => {
                                            return HttpResponse::BadRequest().body("Order side is unspecified");
                                        }
                                    }
                                }
                               
                            } else {//Tsisy ao @ stop order

                                let a_slorder: Result<TraderStopLimitOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_slorder, modify_order.order_identifier).await;

                            if let Ok(a_slorder_s) = a_slorder {
                                if modify_order.trader_identifier != a_slorder_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                                if modify_order.new_quantity > a_slorder_s.order_quantity {
                                    match a_slorder_s.order_side {
                                        OrderSide::Buy => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name,modify_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Buy",)
                                            .await;
                                           let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Buy",)
                                           .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                           }
                                       };
                                       let pending_slorders = match pending_slorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                           }
                                       };
                                       let pending_iceberg = match pending_iceberg_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                           }
                                       };
                                            let mut balance_req = modify_order.new_quantity*a_slorder_s.price as f32;
    
                                            // If there are pending orders, sum their (order_quantity * price)
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity * order.price as f32;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                            for order in pending_sorders {
                                                balance_req += order.order_quantity * order.trigger_price as f32;
                                            }
                                        }
                                        if !pending_slorders.is_empty() {
                                            for order in pending_slorders {
                                                balance_req += order.order_quantity * order.price as f32;
                                            }
                                        }
                                        if !pending_iceberg.is_empty() {
                                            for order in pending_iceberg {
                                                balance_req += order.resting_quantity * order.price as f32;
                                            }
                                        }
                                            
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                        }
                                        OrderSide::Sell => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, modify_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Sell",)
                                            .await;
                                            let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                            }
                                        };
                                        let pending_slorders = match pending_slorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                            }
                                        };
                                        let pending_iceberg = match pending_iceberg_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                            }
                                        };
                                            let mut balance_req = modify_order.new_quantity;
    
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                               for order in pending_sorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_slorders.is_empty() {
                                               for order in pending_slorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_iceberg.is_empty() {
                                               for order in pending_iceberg {
                                                   balance_req += order.resting_quantity;
                                               }
                                           }
        
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                          
                                        }
                                        OrderSide::Unspecified => {
                                            return HttpResponse::BadRequest().body("Order side is unspecified");
                                        }
                                    }
                                }
                               
                            } else { //Tsisy ao @ stop limit order
                                return HttpResponse::InternalServerError().body("The order is not found anywhere.");
                            }
                               
                            }
                               
                            }
                            let json_body = match serde_json::to_string(&data) {
                                Ok(body) => body,
                                Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                            };
                            match verify_hmac_json(&json_body, hmac_str, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::ModifyOrder(data.into_inner())) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn delete_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<DeleteOrder>, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
        let delete_order = data.clone();
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
              
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&delete_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if delete_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_delete_order(&delete_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            let a_order: Result<TraderOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_order, delete_order.order_identifier).await;

                            if let Ok(a_order_s) = a_order {
                                if delete_order.trader_identifier != a_order_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                               
                            } else { //Tsisy ao @ limit order

                                let a_sorder: Result<TraderStopOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_sorder, delete_order.order_identifier).await;

                            if let Ok(a_sorder_s) = a_sorder {
                                if delete_order.trader_identifier != a_sorder_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                               
                            } else {//Tsisy ao @ stop order

                                let a_slorder: Result<TraderStopLimitOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_slorder, delete_order.order_identifier).await;

                            if let Ok(a_slorder_s) = a_slorder {
                                if delete_order.trader_identifier != a_slorder_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                               
                            } else { //Tsisy ao @ stop limit order
                                return HttpResponse::InternalServerError().body("The order is not found anywhere.");
                            }
                               
                            }
                               
                            }
                            let json_body = match serde_json::to_string(&data) {
                                Ok(body) => body,
                                Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                            };
                            match verify_hmac_json(&json_body, hmac_str, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::DeleteOrder(data.into_inner())) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                 
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn modify_iceberg_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<ModifyIcebergOrder>, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
        let modify_iceberg_order = data.clone();
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
              
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&modify_iceberg_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if modify_iceberg_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_modify_iceberg_order(&modify_iceberg_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&modify_iceberg_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_modify_iceberg_specs(&modify_iceberg_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            let a_order: Result<IcebergOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_iceberg, modify_iceberg_order.iceberg_identifier).await;

                            if let Ok(a_order_s) = a_order {
                                if modify_iceberg_order.trader_identifier != a_order_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                                if modify_iceberg_order.new_quantity > a_order_s.resting_quantity {
                                    match a_order_s.order_side {
                                        OrderSide::Buy => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name,modify_iceberg_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_iceberg_order.trader_identifier,"Buy",)
                                            .await;
                                           let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_iceberg_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_iceberg_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_iceberg_order.trader_identifier,"Buy",)
                                           .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                           }
                                       };
                                       let pending_slorders = match pending_slorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                           }
                                       };
                                       let pending_iceberg = match pending_iceberg_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                           }
                                       };
                                            let mut balance_req = modify_iceberg_order.new_quantity*a_order_s.price as f32;
    
                                            // If there are pending orders, sum their (order_quantity * price)
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity * order.price as f32;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                            for order in pending_sorders {
                                                balance_req += order.order_quantity * order.trigger_price as f32;
                                            }
                                        }
                                        if !pending_slorders.is_empty() {
                                            for order in pending_slorders {
                                                balance_req += order.order_quantity * order.price as f32;
                                            }
                                        }
                                        if !pending_iceberg.is_empty() {
                                            for order in pending_iceberg {
                                                balance_req += order.resting_quantity * order.price as f32;
                                            }
                                        }
                                            
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                        }
                                        OrderSide::Sell => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, modify_iceberg_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_iceberg_order.trader_identifier,"Sell",)
                                            .await;
                                            let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_iceberg_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_iceberg_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_iceberg_order.trader_identifier,"Sell",)
                                            .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                            }
                                        };
                                        let pending_slorders = match pending_slorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                            }
                                        };
                                        let pending_iceberg = match pending_iceberg_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                            }
                                        };
                                            let mut balance_req = modify_iceberg_order.new_quantity;
    
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                               for order in pending_sorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_slorders.is_empty() {
                                               for order in pending_slorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_iceberg.is_empty() {
                                               for order in pending_iceberg {
                                                   balance_req += order.resting_quantity;
                                               }
                                           }
        
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                          
                                        }
                                        OrderSide::Unspecified => {
                                            return HttpResponse::BadRequest().body("Order side is unspecified");
                                        }
                                    }
                                }
                               
                            } else {
                                return HttpResponse::InternalServerError().body("The iceberg order is not found.");
                            }
                            let json_body = match serde_json::to_string(&data) {
                                Ok(body) => body,
                                Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                            };
                            match verify_hmac_json(&json_body, hmac_str, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::ModifyIcebergOrder(data.into_inner())) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                   
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn delete_iceberg_order(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, data: web::Json<DeleteIcebergOrder>, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
        let delete_iceberg_order = data.clone();
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
         
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&delete_iceberg_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if delete_iceberg_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_delete_iceberg_order(&delete_iceberg_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            let a_order: Result<IcebergOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_iceberg, delete_iceberg_order.iceberg_identifier).await;

                            if let Ok(a_order_s) = a_order {
                                if delete_iceberg_order.trader_identifier != a_order_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                     
                            } else {
                                return HttpResponse::InternalServerError().body("The iceberg order is not found.");
                            }
                            let json_body = match serde_json::to_string(&data) {
                                Ok(body) => body,
                                Err(_) => return HttpResponse::InternalServerError().body("Failed to serialize data to JSON"),
                            };
                            match verify_hmac_json(&json_body, hmac_str, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::DeleteIcebergOrder(data.into_inner())) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                   
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}
pub async fn save(
    _req: HttpRequest,
    save: web::Json<Save>, // Use web::Json to automatically deserialize JSON
    tx: web::Data<sync_mpsc::Sender<Structs>>,
    market_conf: web::Data<MarketConf>,
) -> impl Responder {
    // Check if the market exists in the market list
    if save.market != market_conf.market_name {
        return HttpResponse::BadRequest().body("Wrong market!");
    }

    // Send the struct to the channel
    if let Err(_) = tx.send(Structs::Save(save.into_inner())) {
        return HttpResponse::InternalServerError().body("Failed to send order");
    }

    HttpResponse::Ok().body("JSON body deserialized successfully and sent to the channel")
}



//Order messagepack handler
pub async fn limit_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match serde_json::from_slice::<LimitOrder>(&body_vec) {
                    Ok(limit_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&limit_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if limit_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            // Retrieve market specifications for the limit order's market
                    
                           
                            if let Err(err) = validate_limit_order(&limit_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&limit_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_limit_specs(&limit_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            match limit_order.order_side {
                                OrderSide::Buy => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, limit_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Buy orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,limit_order.trader_identifier,"Buy",)
                                     .await;
                                    let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,limit_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,limit_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,limit_order.trader_identifier,"Buy",)
                                    .await;
                                 
                                 let pending_orders = match pending_orders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                     }
                                 };
                                 let pending_sorders = match pending_sorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                    }
                                };
                                let pending_slorders = match pending_slorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                    }
                                };
                                let pending_iceberg = match pending_iceberg_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                    }
                                };
                                 
                                 // Calculate the balance requirement for Buy orders
                                 let mut balance_req = limit_order.order_quantity * limit_order.price as f32;
                                 
                                 // If there are pending orders, sum their (order_quantity * price)
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity * order.price as f32;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity * order.trigger_price as f32;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity * order.price as f32;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity * order.price as f32;
                                    }
                                }
                                    
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                }
                                OrderSide::Sell => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, limit_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Sell orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,limit_order.trader_identifier,"Sell",)
                                     .await;
                                     let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,limit_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,limit_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,limit_order.trader_identifier,"Sell",)
                                     .await;
                                  
                                  let pending_orders = match pending_orders_result {
                                      Ok(orders) => orders,
                                      Err(_) => {
                                          return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                      }
                                  };
                                  let pending_sorders = match pending_sorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                     }
                                 };
                                 let pending_slorders = match pending_slorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                     }
                                 };
                                 let pending_iceberg = match pending_iceberg_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                     }
                                 };
                                 
                                 // Calculate the balance requirement for Sell orders
                                 let mut balance_req = limit_order.order_quantity;
                                 
                                 // If there are pending orders, sum their order_quantity
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity;
                                    }
                                }
                                 
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                  
                                }
                                OrderSide::Unspecified => {
                                    return HttpResponse::BadRequest().body("Order side is unspecified");
                                }
                            }
             
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {

                                    if let Err(_) = tx.send(Structs::LimitOrder(limit_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn market_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,bbo:web::Data<Arc<Mutex<BBO>>>) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match rmp_serde::from_slice::<MarketOrder>(&body_vec) {
                    Ok(market_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&market_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if market_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_market_order(&market_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&market_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_market_specs(&market_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            let calc_price: Option<i32>;
                            match bbo.lock() {
                                Ok(bbo_data) => {
                                    // Successfully locked the Mutex, now access the fields of BBO
                                    calc_price = match market_order.order_side {
                                        OrderSide::Buy => bbo_data.ask_price,
                                        OrderSide::Sell => bbo_data.bid_price,
                                        _ => None, // Handle unexpected order side by setting calc_price to None
                                    };
                                }
                                Err(_) => {
                                    // Handle the error if the lock fails
                                    return HttpResponse::InternalServerError().body("Failed to lock BBO data");
                                }
                            }
                            if let Some(price) = calc_price {

                                match market_order.order_side {
                                    OrderSide::Buy => {
                                        let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, market_order.trader_identifier).await;
                                        let balance_s = match balance {
                                        Ok(user) => user, // Extract the user if the result is Ok
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error during balance fetching");
                                            }
                                        };
                                         // Fetch all pending Buy orders for the trader
                                         let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,market_order.trader_identifier,"Buy",)
                                         .await;
                                        let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,market_order.trader_identifier,"Buy",)
                                        .await;
                                    let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,market_order.trader_identifier,"Buy",)
                                        .await;
                                    let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,market_order.trader_identifier,"Buy",)
                                        .await;
                                     
                                     let pending_orders = match pending_orders_result {
                                         Ok(orders) => orders,
                                         Err(_) => {
                                             return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                         }
                                     };
                                     let pending_sorders = match pending_sorders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                        }
                                    };
                                    let pending_slorders = match pending_slorders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                        }
                                    };
                                    let pending_iceberg = match pending_iceberg_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                        }
                                    };
                                     
                                     // Calculate the balance requirement for Buy orders
                                     let mut balance_req = market_order.order_quantity * price as f32;
                                     
                                     // If there are pending orders, sum their (order_quantity * price)
                                     if !pending_orders.is_empty() {
                                         for order in pending_orders {
                                             balance_req += order.order_quantity * order.price as f32;
                                         }
                                     }
                                     if !pending_sorders.is_empty() {
                                        for order in pending_sorders {
                                            balance_req += order.order_quantity * order.trigger_price as f32;
                                        }
                                    }
                                    if !pending_slorders.is_empty() {
                                        for order in pending_slorders {
                                            balance_req += order.order_quantity * order.price as f32;
                                        }
                                    }
                                    if !pending_iceberg.is_empty() {
                                        for order in pending_iceberg {
                                            balance_req += order.resting_quantity * order.price as f32;
                                        }
                                    }
                                        
                                        if balance_s.asset_balance < balance_req {
                                            return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                        }
                                    }
                                    OrderSide::Sell => {
                                        let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, market_order.trader_identifier).await;
                                        let balance_s = match balance {
                                        Ok(user) => user, // Extract the user if the result is Ok
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error during balance fetching");
                                            }
                                        };
                                         // Fetch all pending Sell orders for the trader
                                         let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,market_order.trader_identifier,"Sell",)
                                         .await;
                                         let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,market_order.trader_identifier,"Sell",)
                                         .await;
                                     let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,market_order.trader_identifier,"Sell",)
                                         .await;
                                     let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                         fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,market_order.trader_identifier,"Sell",)
                                         .await;
                                      
                                      let pending_orders = match pending_orders_result {
                                          Ok(orders) => orders,
                                          Err(_) => {
                                              return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                          }
                                      };
                                      let pending_sorders = match pending_sorders_result {
                                         Ok(orders) => orders,
                                         Err(_) => {
                                             return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                         }
                                     };
                                     let pending_slorders = match pending_slorders_result {
                                         Ok(orders) => orders,
                                         Err(_) => {
                                             return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                         }
                                     };
                                     let pending_iceberg = match pending_iceberg_result {
                                         Ok(orders) => orders,
                                         Err(_) => {
                                             return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                         }
                                     };
                                     
                                     // Calculate the balance requirement for Sell orders
                                     let mut balance_req = market_order.order_quantity;
                                     
                                     // If there are pending orders, sum their order_quantity
                                     if !pending_orders.is_empty() {
                                         for order in pending_orders {
                                             balance_req += order.order_quantity;
                                         }
                                     }
                                     if !pending_sorders.is_empty() {
                                        for order in pending_sorders {
                                            balance_req += order.order_quantity;
                                        }
                                    }
                                    if !pending_slorders.is_empty() {
                                        for order in pending_slorders {
                                            balance_req += order.order_quantity;
                                        }
                                    }
                                    if !pending_iceberg.is_empty() {
                                        for order in pending_iceberg {
                                            balance_req += order.resting_quantity;
                                        }
                                    }
                                     
                                        if balance_s.asset_balance < balance_req {
                                            return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                        }
                                      
                                    }
                                    OrderSide::Unspecified => {
                                        return HttpResponse::BadRequest().body("Order side is unspecified");
                                    }
                                }
                               
                            } else {
                                // Handle the case where the price is None (i.e., no valid ask_price or bid_price found)
                                return HttpResponse::BadRequest().body("Invalid price: ask or bid price is not available.");
                            }
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {
                                    if let Err(_) = tx.send(Structs::MarketOrder(market_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn stop_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes,tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match rmp_serde::from_slice::<StopOrder>(&body_vec) {
                    Ok(stop_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&stop_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if stop_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_stop_order(&stop_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&stop_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_stop_specs(&stop_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            match stop_order.order_side {
                                OrderSide::Buy => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, stop_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Buy orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,stop_order.trader_identifier,"Buy",)
                                     .await;
                                    let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,stop_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,stop_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,stop_order.trader_identifier,"Buy",)
                                    .await;
                                 
                                 let pending_orders = match pending_orders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                     }
                                 };
                                 let pending_sorders = match pending_sorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                    }
                                };
                                let pending_slorders = match pending_slorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                    }
                                };
                                let pending_iceberg = match pending_iceberg_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                    }
                                };
                                 
                                 // Calculate the balance requirement for Buy orders
                                 let mut balance_req = stop_order.order_quantity * stop_order.trigger_price as f32;
                                 
                                 // If there are pending orders, sum their (order_quantity * price)
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity * order.price as f32;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity * order.trigger_price as f32;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity * order.price as f32;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity * order.price as f32;
                                    }
                                }
                                    
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                }
                                OrderSide::Sell => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, stop_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Sell orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,stop_order.trader_identifier,"Sell",)
                                     .await;
                                     let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,stop_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,stop_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,stop_order.trader_identifier,"Sell",)
                                     .await;
                                  
                                  let pending_orders = match pending_orders_result {
                                      Ok(orders) => orders,
                                      Err(_) => {
                                          return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                      }
                                  };
                                  let pending_sorders = match pending_sorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                     }
                                 };
                                 let pending_slorders = match pending_slorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                     }
                                 };
                                 let pending_iceberg = match pending_iceberg_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                     }
                                 };
                                 
                                 // Calculate the balance requirement for Sell orders
                                 let mut balance_req = stop_order.order_quantity;
                                 
                                 // If there are pending orders, sum their order_quantity
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity;
                                    }
                                }
                                 
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                  
                                }
                                OrderSide::Unspecified => {
                                    return HttpResponse::BadRequest().body("Order side is unspecified");
                                }
                            }
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {
                                    if let Err(_) = tx.send(Structs::StopOrder(stop_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn stoplimit_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match rmp_serde::from_slice::<StopLimitOrder>(&body_vec) {
                    Ok(stoplimit_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&stoplimit_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if stoplimit_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_stoplimit_order(&stoplimit_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&stoplimit_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_stoplimit_specs(&stoplimit_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            match stoplimit_order.order_side {
                                OrderSide::Buy => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, stoplimit_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Buy orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,stoplimit_order.trader_identifier,"Buy",)
                                     .await;
                                    let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,stoplimit_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,stoplimit_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,stoplimit_order.trader_identifier,"Buy",)
                                    .await;
                                 
                                 let pending_orders = match pending_orders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                     }
                                 };
                                 let pending_sorders = match pending_sorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                    }
                                };
                                let pending_slorders = match pending_slorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                    }
                                };
                                let pending_iceberg = match pending_iceberg_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                    }
                                };
                                 
                                 // Calculate the balance requirement for Buy orders
                                 let mut balance_req = stoplimit_order.order_quantity * stoplimit_order.price as f32;
                                 
                                 // If there are pending orders, sum their (order_quantity * price)
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity * order.price as f32;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity * order.trigger_price as f32;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity * order.price as f32;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity * order.price as f32;
                                    }
                                }
                                    
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                }
                                OrderSide::Sell => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, stoplimit_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Sell orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,stoplimit_order.trader_identifier,"Sell",)
                                     .await;
                                     let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,stoplimit_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,stoplimit_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,stoplimit_order.trader_identifier,"Sell",)
                                     .await;
                                  
                                  let pending_orders = match pending_orders_result {
                                      Ok(orders) => orders,
                                      Err(_) => {
                                          return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                      }
                                  };
                                  let pending_sorders = match pending_sorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                     }
                                 };
                                 let pending_slorders = match pending_slorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                     }
                                 };
                                 let pending_iceberg = match pending_iceberg_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                     }
                                 };
                                 
                                 // Calculate the balance requirement for Sell orders
                                 let mut balance_req = stoplimit_order.order_quantity;
                                 
                                 // If there are pending orders, sum their order_quantity
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity;
                                    }
                                }
                                 
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                  
                                }
                                OrderSide::Unspecified => {
                                    return HttpResponse::BadRequest().body("Order side is unspecified");
                                }
                            }
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {
                                    if let Err(_) = tx.send(Structs::StopLimitOrder(stoplimit_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn modify_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match rmp_serde::from_slice::<ModifyOrder>(&body_vec) {
                    Ok(modify_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&modify_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if modify_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_modify_order(&modify_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&modify_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_modify_specs(&modify_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            let a_order: Result<TraderOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_order, modify_order.order_identifier).await;

                            if let Ok(a_order_s) = a_order {
                                if modify_order.trader_identifier != a_order_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                                if modify_order.new_quantity > a_order_s.order_quantity {
                                match a_order_s.order_side {
                                    OrderSide::Buy => {
                                        let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name,modify_order.trader_identifier).await;
                                        let balance_s = match balance {
                                        Ok(user) => user, // Extract the user if the result is Ok
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error during balance fetching");
                                            }
                                        };
                                        let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Buy",)
                                        .await;
                                       let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                       fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Buy",)
                                       .await;
                                   let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                       fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Buy",)
                                       .await;
                                   let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                       fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Buy",)
                                       .await;
                                    
                                    let pending_orders = match pending_orders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                        }
                                    };
                                    let pending_sorders = match pending_sorders_result {
                                       Ok(orders) => orders,
                                       Err(_) => {
                                           return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                       }
                                   };
                                   let pending_slorders = match pending_slorders_result {
                                       Ok(orders) => orders,
                                       Err(_) => {
                                           return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                       }
                                   };
                                   let pending_iceberg = match pending_iceberg_result {
                                       Ok(orders) => orders,
                                       Err(_) => {
                                           return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                       }
                                   };
                                        let mut balance_req = modify_order.new_quantity*a_order_s.price as f32;

                                        // If there are pending orders, sum their (order_quantity * price)
                                        if !pending_orders.is_empty() {
                                            for order in pending_orders {
                                                balance_req += order.order_quantity * order.price as f32;
                                            }
                                        }
                                        if !pending_sorders.is_empty() {
                                        for order in pending_sorders {
                                            balance_req += order.order_quantity * order.trigger_price as f32;
                                        }
                                    }
                                    if !pending_slorders.is_empty() {
                                        for order in pending_slorders {
                                            balance_req += order.order_quantity * order.price as f32;
                                        }
                                    }
                                    if !pending_iceberg.is_empty() {
                                        for order in pending_iceberg {
                                            balance_req += order.resting_quantity * order.price as f32;
                                        }
                                    }
                                        
                                        if balance_s.asset_balance < balance_req {
                                            return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                        }
                                    }
                                    OrderSide::Sell => {
                                        let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, modify_order.trader_identifier).await;
                                        let balance_s = match balance {
                                        Ok(user) => user, // Extract the user if the result is Ok
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error during balance fetching");
                                            }
                                        };
                                        let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Sell",)
                                        .await;
                                        let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Sell",)
                                        .await;
                                    let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Sell",)
                                        .await;
                                    let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                        fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Sell",)
                                        .await;
                                    
                                    let pending_orders = match pending_orders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                        }
                                    };
                                    let pending_sorders = match pending_sorders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                        }
                                    };
                                    let pending_slorders = match pending_slorders_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                        }
                                    };
                                    let pending_iceberg = match pending_iceberg_result {
                                        Ok(orders) => orders,
                                        Err(_) => {
                                            return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                        }
                                    };
                                        let mut balance_req = modify_order.new_quantity;

                                        if !pending_orders.is_empty() {
                                            for order in pending_orders {
                                                balance_req += order.order_quantity;
                                            }
                                        }
                                        if !pending_sorders.is_empty() {
                                           for order in pending_sorders {
                                               balance_req += order.order_quantity;
                                           }
                                       }
                                       if !pending_slorders.is_empty() {
                                           for order in pending_slorders {
                                               balance_req += order.order_quantity;
                                           }
                                       }
                                       if !pending_iceberg.is_empty() {
                                           for order in pending_iceberg {
                                               balance_req += order.resting_quantity;
                                           }
                                       }
    
                                        if balance_s.asset_balance < balance_req {
                                            return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                        }
                                      
                                    }
                                    OrderSide::Unspecified => {
                                        return HttpResponse::BadRequest().body("Order side is unspecified");
                                    }
                                }
                            }
                               
                            } else { //Tsisy ao @ limit order

                                let a_sorder: Result<TraderStopOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_sorder, modify_order.order_identifier).await;

                            if let Ok(a_sorder_s) = a_sorder {
                                if modify_order.trader_identifier != a_sorder_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                                if modify_order.new_quantity > a_sorder_s.order_quantity {
                                    match a_sorder_s.order_side {
                                        OrderSide::Buy => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name,modify_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Buy",)
                                            .await;
                                           let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Buy",)
                                           .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                           }
                                       };
                                       let pending_slorders = match pending_slorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                           }
                                       };
                                       let pending_iceberg = match pending_iceberg_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                           }
                                       };
                                            let mut balance_req = modify_order.new_quantity*a_sorder_s.trigger_price as f32;
    
                                            // If there are pending orders, sum their (order_quantity * price)
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity * order.price as f32;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                            for order in pending_sorders {
                                                balance_req += order.order_quantity * order.trigger_price as f32;
                                            }
                                        }
                                        if !pending_slorders.is_empty() {
                                            for order in pending_slorders {
                                                balance_req += order.order_quantity * order.price as f32;
                                            }
                                        }
                                        if !pending_iceberg.is_empty() {
                                            for order in pending_iceberg {
                                                balance_req += order.resting_quantity * order.price as f32;
                                            }
                                        }
                                            
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                        }
                                        OrderSide::Sell => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, modify_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Sell",)
                                            .await;
                                            let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                            }
                                        };
                                        let pending_slorders = match pending_slorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                            }
                                        };
                                        let pending_iceberg = match pending_iceberg_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                            }
                                        };
                                            let mut balance_req = modify_order.new_quantity;
    
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                               for order in pending_sorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_slorders.is_empty() {
                                               for order in pending_slorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_iceberg.is_empty() {
                                               for order in pending_iceberg {
                                                   balance_req += order.resting_quantity;
                                               }
                                           }
        
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                          
                                        }
                                        OrderSide::Unspecified => {
                                            return HttpResponse::BadRequest().body("Order side is unspecified");
                                        }
                                    }
                                }
                               
                            } else {//Tsisy ao @ stop order

                                let a_slorder: Result<TraderStopLimitOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_slorder, modify_order.order_identifier).await;

                            if let Ok(a_slorder_s) = a_slorder {
                                if modify_order.trader_identifier != a_slorder_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                                if modify_order.new_quantity > a_slorder_s.order_quantity {
                                    match a_slorder_s.order_side {
                                        OrderSide::Buy => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name,modify_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Buy",)
                                            .await;
                                           let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Buy",)
                                           .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                           }
                                       };
                                       let pending_slorders = match pending_slorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                           }
                                       };
                                       let pending_iceberg = match pending_iceberg_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                           }
                                       };
                                            let mut balance_req = modify_order.new_quantity*a_slorder_s.price as f32;
    
                                            // If there are pending orders, sum their (order_quantity * price)
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity * order.price as f32;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                            for order in pending_sorders {
                                                balance_req += order.order_quantity * order.trigger_price as f32;
                                            }
                                        }
                                        if !pending_slorders.is_empty() {
                                            for order in pending_slorders {
                                                balance_req += order.order_quantity * order.price as f32;
                                            }
                                        }
                                        if !pending_iceberg.is_empty() {
                                            for order in pending_iceberg {
                                                balance_req += order.resting_quantity * order.price as f32;
                                            }
                                        }
                                            
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                        }
                                        OrderSide::Sell => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, modify_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_order.trader_identifier,"Sell",)
                                            .await;
                                            let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_order.trader_identifier,"Sell",)
                                            .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                            }
                                        };
                                        let pending_slorders = match pending_slorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                            }
                                        };
                                        let pending_iceberg = match pending_iceberg_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                            }
                                        };
                                            let mut balance_req = modify_order.new_quantity;
    
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                               for order in pending_sorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_slorders.is_empty() {
                                               for order in pending_slorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_iceberg.is_empty() {
                                               for order in pending_iceberg {
                                                   balance_req += order.resting_quantity;
                                               }
                                           }
        
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                          
                                        }
                                        OrderSide::Unspecified => {
                                            return HttpResponse::BadRequest().body("Order side is unspecified");
                                        }
                                    }
                                }
                               
                            } else { //Tsisy ao @ stop limit order
                                return HttpResponse::InternalServerError().body("The order is not found anywhere.");
                            }
                               
                            }
                               
                            }
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {
                                    if let Err(_) = tx.send(Structs::ModifyOrder(modify_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn delete_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match rmp_serde::from_slice::<DeleteOrder>(&body_vec) {
                    Ok(delete_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&delete_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if delete_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_delete_order(&delete_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            let a_order: Result<TraderOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_order, delete_order.order_identifier).await;

                            if let Ok(a_order_s) = a_order {
                                if delete_order.trader_identifier != a_order_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                               
                            } else { //Tsisy ao @ limit order

                                let a_sorder: Result<TraderStopOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_sorder, delete_order.order_identifier).await;

                            if let Ok(a_sorder_s) = a_sorder {
                                if delete_order.trader_identifier != a_sorder_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                               
                            } else {//Tsisy ao @ stop order

                                let a_slorder: Result<TraderStopLimitOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_slorder, delete_order.order_identifier).await;

                            if let Ok(a_slorder_s) = a_slorder {
                                if delete_order.trader_identifier != a_slorder_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                               
                            } else { //Tsisy ao @ stop limit order
                                return HttpResponse::InternalServerError().body("THe order is not found anywhere.");
                            }
                               
                            }
                               
                            }
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {
                                    if let Err(_) = tx.send(Structs::DeleteOrder(delete_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn iceberg_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match rmp_serde::from_slice::<IcebergOrder>(&body_vec) {
                    Ok(iceberg_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&iceberg_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if iceberg_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            // Retrieve market specifications for the limit order's market
                           
                            if let Err(err) = validate_iceberg_order(&iceberg_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&iceberg_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_iceberg_specs(&iceberg_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                           
                            match iceberg_order.order_side {
                                OrderSide::Buy => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name, iceberg_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Buy orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,iceberg_order.trader_identifier,"Buy",)
                                     .await;
                                    let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,iceberg_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,iceberg_order.trader_identifier,"Buy",)
                                    .await;
                                let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                    fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,iceberg_order.trader_identifier,"Buy",)
                                    .await;
                                 
                                 let pending_orders = match pending_orders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                     }
                                 };
                                 let pending_sorders = match pending_sorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                    }
                                };
                                let pending_slorders = match pending_slorders_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                    }
                                };
                                let pending_iceberg = match pending_iceberg_result {
                                    Ok(orders) => orders,
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                    }
                                };
                                 
                                 // Calculate the balance requirement for Buy orders
                                 let mut balance_req = iceberg_order.total_quantity * iceberg_order.price as f32;
                                 
                                 // If there are pending orders, sum their (order_quantity * price)
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity * order.price as f32;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity * order.trigger_price as f32;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity * order.price as f32;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity * order.price as f32;
                                    }
                                }
                                    
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                }
                                OrderSide::Sell => {
                                    let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, iceberg_order.trader_identifier).await;
                                    let balance_s = match balance {
                                    Ok(user) => user, // Extract the user if the result is Ok
                                    Err(_) => {
                                        return HttpResponse::InternalServerError().body("Error during balance fetching");
                                        }
                                    };
                                     // Fetch all pending Sell orders for the trader
                                     let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,iceberg_order.trader_identifier,"Sell",)
                                     .await;
                                     let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,iceberg_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,iceberg_order.trader_identifier,"Sell",)
                                     .await;
                                 let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                     fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,iceberg_order.trader_identifier,"Sell",)
                                     .await;
                                  
                                  let pending_orders = match pending_orders_result {
                                      Ok(orders) => orders,
                                      Err(_) => {
                                          return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                      }
                                  };
                                  let pending_sorders = match pending_sorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                     }
                                 };
                                 let pending_slorders = match pending_slorders_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                     }
                                 };
                                 let pending_iceberg = match pending_iceberg_result {
                                     Ok(orders) => orders,
                                     Err(_) => {
                                         return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                     }
                                 };
                                 
                                 // Calculate the balance requirement for Sell orders
                                 let mut balance_req = iceberg_order.total_quantity;
                                 
                                 // If there are pending orders, sum their order_quantity
                                 if !pending_orders.is_empty() {
                                     for order in pending_orders {
                                         balance_req += order.order_quantity;
                                     }
                                 }
                                 if !pending_sorders.is_empty() {
                                    for order in pending_sorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_slorders.is_empty() {
                                    for order in pending_slorders {
                                        balance_req += order.order_quantity;
                                    }
                                }
                                if !pending_iceberg.is_empty() {
                                    for order in pending_iceberg {
                                        balance_req += order.resting_quantity;
                                    }
                                }
                                 
                                    if balance_s.asset_balance < balance_req {
                                        return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the trade as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                    }
                                  
                                }
                                OrderSide::Unspecified => {
                                    return HttpResponse::BadRequest().body("Order side is unspecified");
                                }
                            }       
                            
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {
                                    if let Err(_) = tx.send(Structs::IcebergOrder(iceberg_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn modify_iceberg_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,
    market_spec_config: web::Data<MarketSpecConfig>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match rmp_serde::from_slice::<ModifyIcebergOrder>(&body_vec) {
                    Ok(modify_iceberg_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&modify_iceberg_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if modify_iceberg_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_modify_iceberg_order(&modify_iceberg_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            if let Some(market_spec) = market_spec_config.market_specification.get(&modify_iceberg_order.market) {
                                // Validate the order against market specs
                                if let Err(err) = validate_modify_iceberg_specs(&modify_iceberg_order, market_spec) {
                                    return HttpResponse::BadRequest().body(err);
                                }
                            } else {
                                return HttpResponse::BadRequest().body("Market specification not found");
                            }
                            let a_order: Result<IcebergOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_iceberg, modify_iceberg_order.iceberg_identifier).await;

                            if let Ok(a_order_s) = a_order {
                                if modify_iceberg_order.trader_identifier != a_order_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                                if modify_iceberg_order.new_quantity > a_order_s.resting_quantity {
                                    match a_order_s.order_side {
                                        OrderSide::Buy => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_b_name,modify_iceberg_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_iceberg_order.trader_identifier,"Buy",)
                                            .await;
                                           let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_iceberg_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_iceberg_order.trader_identifier,"Buy",)
                                           .await;
                                       let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                           fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_iceberg_order.trader_identifier,"Buy",)
                                           .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                           }
                                       };
                                       let pending_slorders = match pending_slorders_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                           }
                                       };
                                       let pending_iceberg = match pending_iceberg_result {
                                           Ok(orders) => orders,
                                           Err(_) => {
                                               return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                           }
                                       };
                                            let mut balance_req = modify_iceberg_order.new_quantity*a_order_s.price as f32;
    
                                            // If there are pending orders, sum their (order_quantity * price)
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity * order.price as f32;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                            for order in pending_sorders {
                                                balance_req += order.order_quantity * order.trigger_price as f32;
                                            }
                                        }
                                        if !pending_slorders.is_empty() {
                                            for order in pending_slorders {
                                                balance_req += order.order_quantity * order.price as f32;
                                            }
                                        }
                                        if !pending_iceberg.is_empty() {
                                            for order in pending_iceberg {
                                                balance_req += order.resting_quantity * order.price as f32;
                                            }
                                        }
                                            
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                        }
                                        OrderSide::Sell => {
                                            let balance: Result<TraderBalance, String> = fetch_balance_traderid(&db, &coll_config.coll_trdr_bal,&market_conf.asset_a_name, modify_iceberg_order.trader_identifier).await;
                                            let balance_s = match balance {
                                            Ok(user) => user, // Extract the user if the result is Ok
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error during balance fetching");
                                                }
                                            };
                                            let pending_orders_result: Result<Vec<TraderOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_order,modify_iceberg_order.trader_identifier,"Sell",)
                                            .await;
                                            let pending_sorders_result: Result<Vec<TraderStopOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_sorder,modify_iceberg_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_slorders_result: Result<Vec<TraderStopLimitOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_slorder,modify_iceberg_order.trader_identifier,"Sell",)
                                            .await;
                                        let pending_iceberg_result: Result<Vec<IcebergOrderStruct>, String> =
                                            fetch_all_pendingorder_withorderside_by_trader_id(&db,&coll_config.coll_p_iceberg,modify_iceberg_order.trader_identifier,"Sell",)
                                            .await;
                                        
                                        let pending_orders = match pending_orders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending orders");
                                            }
                                        };
                                        let pending_sorders = match pending_sorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop orders");
                                            }
                                        };
                                        let pending_slorders = match pending_slorders_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending Stop limit orders");
                                            }
                                        };
                                        let pending_iceberg = match pending_iceberg_result {
                                            Ok(orders) => orders,
                                            Err(_) => {
                                                return HttpResponse::InternalServerError().body("Error fetching pending iceberg orders");
                                            }
                                        };
                                            let mut balance_req = modify_iceberg_order.new_quantity;
    
                                            if !pending_orders.is_empty() {
                                                for order in pending_orders {
                                                    balance_req += order.order_quantity;
                                                }
                                            }
                                            if !pending_sorders.is_empty() {
                                               for order in pending_sorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_slorders.is_empty() {
                                               for order in pending_slorders {
                                                   balance_req += order.order_quantity;
                                               }
                                           }
                                           if !pending_iceberg.is_empty() {
                                               for order in pending_iceberg {
                                                   balance_req += order.resting_quantity;
                                               }
                                           }
        
                                            if balance_s.asset_balance < balance_req {
                                                return HttpResponse::BadRequest().body(format!("Not sufficient balance to conduct the modification as the requirement is {} {}.",balance_req,balance_s.asset_name));
                                            }
                                          
                                        }
                                        OrderSide::Unspecified => {
                                            return HttpResponse::BadRequest().body("Order side is unspecified");
                                        }
                                    }
                                }
                               
                            } else {
                                return HttpResponse::InternalServerError().body("The iceberg order is not found.");
                            }
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {
                                    if let Err(_) = tx.send(Structs::ModifyIcebergOrder(modify_iceberg_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn delete_iceberg_order_msgp(broker_db: web::Data<Arc<BrokerDb>>,req: HttpRequest, body: web::Bytes, tx: web::Data<sync_mpsc::Sender<Structs>>,
    broker_config: web::Data<BrokerConfigDedicaced>,
    market_conf: web::Data<MarketConf>,coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder {
        let db: &Database = &broker_db.db;
        let received_hmac = req.headers().get("X-HMAC-SIGNATURE");
    
        if let Some(hmac_value) = received_hmac {
            if let Ok(hmac_str) = hmac_value.to_str() {
                let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    
                // Deserialize the body to extract the broker identifier
                match rmp_serde::from_slice::<DeleteIcebergOrder>(&body_vec) {
                    Ok(delete_iceberg_order) => {
                        // Lookup the broker's HMAC key in the broker config
                        if let Some(hmac_key) = broker_config.broker_list.get(&delete_iceberg_order.broker_identifier) {
                            // Verify the HMAC signature using the broker-specific key
                            if delete_iceberg_order.market != market_conf.market_name {
                                return HttpResponse::BadRequest().body("Wrong market!");
                            }
                            if let Err(err) = validate_delete_iceberg_order(&delete_iceberg_order) {
                                return HttpResponse::BadRequest().body(err);
                            }
                            let a_order: Result<IcebergOrderStruct, String> = fetch_document_byorderid(&db, &coll_config.coll_p_iceberg, delete_iceberg_order.iceberg_identifier).await;

                            if let Ok(a_order_s) = a_order {
                                if delete_iceberg_order.trader_identifier != a_order_s.trader_identifier {
                                    return HttpResponse::BadRequest().body("The order is not owned by the user.");
                                }
                     
                            } else {
                                return HttpResponse::InternalServerError().body("The iceberg order is not found.");
                            }
                            match verify_hmac(&body_vec, hmac_str, hmac_key) {
                                Ok(true) => {
                                    if let Err(_) = tx.send(Structs::DeleteIcebergOrder(delete_iceberg_order)) {
                                        return HttpResponse::InternalServerError().body("Failed to send order");
                                    }
                                    return HttpResponse::Ok().body("HMAC verified and body deserialized successfully");
                                }
                                Ok(false) => return HttpResponse::Unauthorized().body("Invalid HMAC signature"),
                                Err(_) => return HttpResponse::BadRequest().body("HMAC verification failed"),
                            }
                        } else {
                            return HttpResponse::BadRequest().body("Unknown Broker!");
                        }
                    }
                    Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
                }
            } else {
                return HttpResponse::BadRequest().body("Invalid HMAC header");
            }
        } else {
            return HttpResponse::BadRequest().body("No HMAC signature provided");
        }
}

pub async fn save_msgp(
    _req: HttpRequest, // Removed HMAC header handling
    body: web::Bytes,
    tx: web::Data<Arc<mpsc::UnboundedSender<Structs>>>,
    market_conf: web::Data<MarketConf>, // Removed broker_config since it's no longer needed
) -> impl Responder {
    let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>

    // Deserialize the body to extract the broker identifier
    match rmp_serde::from_slice::<Save>(&body_vec) {
        Ok(save) => {
            // Check if the market exists in the market list
            if save.market != market_conf.market_name {
                return HttpResponse::BadRequest().body("Wrong market!");
            }
            // Send the struct to the channel
            if let Err(_) = tx.send(Structs::Save(save)) {
                return HttpResponse::InternalServerError().body("Failed to send order");
            }
            HttpResponse::Ok().body("Body deserialized successfully and sent to the channel")
        }
        Err(_) => HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
    }
}


pub async fn history_last(data: web::Json<NumberToFetch>,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,)-> impl Responder {

    /*let coll_h_last = match env::var("COLL_H_LAST") {
        Ok(name) => name,
        Err(err) => {
            eprintln!("COLLECTION_NAME_SENSITIVE_ERROR: {}", err);
            return HttpResponse::InternalServerError()
            .json(json!({
                "Error": "Internal Server Error",
               
            }));
        }
    };*/
    let db: &Database = &market_db.db;
    match fetch_number_documents::<Last>(&db, &coll_config.coll_h_last, data.number).await {
        Ok(documents) => HttpResponse::Ok().json(json!({
            "success": true,
            "data": documents,  // Return the documents as JSON
        })),
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().json(json!({
                "success": false,
                "error": "Failed to fetch documents",
            }))
        }
    }
   
}
pub async fn history_bbo(data: web::Json<NumberToFetch>,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder{

    /*let coll_h_bbo = match env::var("COLL_H_BBO") {
        Ok(name) => name,
        Err(err) => {
            eprintln!("COLLECTION_NAME_SENSITIVE_ERROR: {}", err);
            return HttpResponse::InternalServerError()
            .json(json!({
                "Error": "Internal Server Error",
               
            }));
        }
    };*/
    let db: &Database = &market_db.db;
    match fetch_number_documents::<BBO>(&db, &coll_config.coll_h_bbo, data.number).await {
        Ok(documents) => HttpResponse::Ok().json(json!({
            "success": true,
            "data": documents,  // Return the documents as JSON
        })),
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().json(json!({
                "success": false,
                "error": "Failed to fetch documents",
            }))
        }
    }
   
}
pub async fn history_tns(data: web::Json<NumberToFetch>,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,)-> impl Responder {

   /* let coll_h_tns = match env::var("COLL_H_TNS") {
        Ok(name) => name,
        Err(err) => {
            eprintln!("COLLECTION_NAME_SENSITIVE_ERROR: {}", err);
            return HttpResponse::InternalServerError()
            .json(json!({
                "Error": "Internal Server Error",
               
            }));
        }
    };*/
    let db: &Database = &market_db.db;
    match fetch_number_documents::<TimeSale>(&db, &coll_config.coll_h_tns, data.number).await {
        Ok(documents) => HttpResponse::Ok().json(json!({
            "success": true,
            "data": documents,  // Return the documents as JSON
        })),
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().json(json!({
                "success": false,
                "error": "Failed to fetch documents",
            }))
        }
    }
    
   
}
pub async fn history_mbpevent(data: web::Json<NumberToFetch>,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder{

    /*let coll_h_mbp_event = match env::var("COLL_H_MBP_EVENT") {
        Ok(name) => name,
        Err(err) => {
            eprintln!("COLLECTION_NAME_SENSITIVE_ERROR: {}", err);
            return HttpResponse::InternalServerError()
            .json(json!({
                "Error": "Internal Server Error",
               
            }));
        }
    };*/
    let db: &Database = &market_db.db;
    match fetch_number_documents::<MBPEvents>(&db, &coll_config.coll_h_mbpevent, data.number).await {
        Ok(documents) => HttpResponse::Ok().json(json!({
            "success": true,
            "data": documents,  // Return the documents as JSON
        })),
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().json(json!({
                "success": false,
                "error": "Failed to fetch documents",
            }))
        }
    }
   
}
pub async fn history_volume(data: web::Json<NumberToFetch>,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,)-> impl Responder {

    /*let coll_h_volume = match env::var("COLL_H_VOLUME") {
        Ok(name) => name,
        Err(err) => {
            eprintln!("COLLECTION_NAME_SENSITIVE_ERROR: {}", err);
            return HttpResponse::InternalServerError()
            .json(json!({
                "Error": "Internal Server Error",
               
            }));
        }
    };*/
    let db: &Database = &market_db.db;
    match fetch_number_documents::<Volume>(&db, &coll_config.coll_h_volume, data.number).await {
        Ok(documents) => HttpResponse::Ok().json(json!({
            "success": true,
            "data": documents,  // Return the documents as JSON
        })),
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().json(json!({
                "success": false,
                "error": "Failed to fetch documents",
            }))
        }
    }
   
}
pub async fn full_ob_extractor(market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,)-> impl Responder {

    /*let coll_full_ob = match env::var("COLL_FULL_OB") {
        Ok(name) => name,
        Err(err) => {
            eprintln!("COLLECTION_NAME_SENSITIVE_ERROR: {}", err);
            return HttpResponse::InternalServerError()
            .json(json!({
                "Error": "Internal Server Error",
               
            }));
        }
    };*/
    let db: &Database = &market_db.db;
    match fetch_one_document::<FullOB>(&db, &coll_config.coll_fullob).await {
        Ok(documents) => HttpResponse::Ok().json(json!({
            "success": true,
            "data": documents,  // Return the documents as JSON
        })),
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().json(json!({
                "success": false,
                "error": "Failed to fetch documents",
            }))
        }
    }
   
}
 

//Historical data handler messagepack
pub async fn history_last_msgp(body: web::Bytes,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,)-> impl Responder {

    let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    let db: &Database = &market_db.db;
    let data: NumberToFetch = match rmp_serde::from_slice(&body_vec) {
        Ok(data) => data,
        Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
    };
    match fetch_number_documents::<Last>(&db, &coll_config.coll_h_last, data.number).await {
        Ok(documents) => {
            // Serialize the documents to MessagePack format
            match to_vec_named(&documents) {
                Ok(msgpack_data) => HttpResponse::Ok()
                    .content_type("application/msgpack") // Set the content type to MessagePack
                    .body(msgpack_data), // Send the MessagePack data as the response
                Err(err) => {
                    eprintln!("Error serializing documents to MessagePack: {}", err);
                    HttpResponse::InternalServerError().body("Failed to serialize documents")
                }
            }
        }
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().body("Failed to fetch documents")
        }
    }
   
}
pub async fn history_bbo_msgp(body: web::Bytes,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder{

    let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    let db: &Database = &market_db.db;
    let data: NumberToFetch = match rmp_serde::from_slice(&body_vec) {
        Ok(data) => data,
        Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
    };
    match fetch_number_documents::<BBO>(&db, &coll_config.coll_h_bbo, data.number).await {
        Ok(documents) => {
            // Serialize the documents to MessagePack format
            match to_vec_named(&documents) {
                Ok(msgpack_data) => HttpResponse::Ok()
                    .content_type("application/msgpack") // Set the content type to MessagePack
                    .body(msgpack_data), // Send the MessagePack data as the response
                Err(err) => {
                    eprintln!("Error serializing documents to MessagePack: {}", err);
                    HttpResponse::InternalServerError().body("Failed to serialize documents")
                }
            }
        }
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().body("Failed to fetch documents")
        }
    }
   
}
pub async fn history_tns_msgp(body: web::Bytes,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,)-> impl Responder {

    let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    let db: &Database = &market_db.db;
    let data: NumberToFetch = match rmp_serde::from_slice(&body_vec) {
        Ok(data) => data,
        Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
    };
    match fetch_number_documents::<TimeSale>(&db, &coll_config.coll_h_tns, data.number).await {
        Ok(documents) => {
            // Serialize the documents to MessagePack format
            match to_vec_named(&documents) {
                Ok(msgpack_data) => HttpResponse::Ok()
                    .content_type("application/msgpack") // Set the content type to MessagePack
                    .body(msgpack_data), // Send the MessagePack data as the response
                Err(err) => {
                    eprintln!("Error serializing documents to MessagePack: {}", err);
                    HttpResponse::InternalServerError().body("Failed to serialize documents")
                }
            }
        }
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().body("Failed to fetch documents")
        }
    }
    
   
}
pub async fn history_mbpevent_msgp(body: web::Bytes,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,) -> impl Responder{

    let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    let db: &Database = &market_db.db;
    let data: NumberToFetch = match rmp_serde::from_slice(&body_vec) {
        Ok(data) => data,
        Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
    };
    match fetch_number_documents::<MBPEvents>(&db, &coll_config.coll_h_mbpevent, data.number).await {
        Ok(documents) => {
            // Serialize the documents to MessagePack format
            match to_vec_named(&documents) {
                Ok(msgpack_data) => HttpResponse::Ok()
                    .content_type("application/msgpack") // Set the content type to MessagePack
                    .body(msgpack_data), // Send the MessagePack data as the response
                Err(err) => {
                    eprintln!("Error serializing documents to MessagePack: {}", err);
                    HttpResponse::InternalServerError().body("Failed to serialize documents")
                }
            }
        }
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().body("Failed to fetch documents")
        }
    }
   
}
pub async fn history_volume_msgp(body: web::Bytes,market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,)-> impl Responder {

    let body_vec = body.to_vec(); // Convert Bytes to Vec<u8>
    let db: &Database = &market_db.db;
    let data: NumberToFetch = match rmp_serde::from_slice(&body_vec) {
        Ok(data) => data,
        Err(_) => return HttpResponse::BadRequest().body("Failed to deserialize MessagePack"),
    };
    match fetch_number_documents::<Volume>(&db, &coll_config.coll_h_volume, data.number).await {
        Ok(documents) => {
            // Serialize the documents to MessagePack format
            match to_vec_named(&documents) {
                Ok(msgpack_data) => HttpResponse::Ok()
                    .content_type("application/msgpack") // Set the content type to MessagePack
                    .body(msgpack_data), // Send the MessagePack data as the response
                Err(err) => {
                    eprintln!("Error serializing documents to MessagePack: {}", err);
                    HttpResponse::InternalServerError().body("Failed to serialize documents")
                }
            }
        }
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().body("Failed to fetch documents")
        }
    }
   
}
pub async fn full_ob_extractor_msgp(market_db: web::Data<Arc<MarketDb>>, coll_config: web::Data<Arc<CollConfig>>,)-> impl Responder {

    
    let db: &Database = &market_db.db;
    match fetch_one_document::<FullOB>(&db, &coll_config.coll_fullob).await {
        Ok(documents) => {
            // Serialize the documents to MessagePack format
            match to_vec_named(&documents) {
                Ok(msgpack_data) => HttpResponse::Ok()
                    .content_type("application/msgpack") // Set the content type to MessagePack
                    .body(msgpack_data), // Send the MessagePack data as the response
                Err(err) => {
                    eprintln!("Error serializing documents to MessagePack: {}", err);
                    HttpResponse::InternalServerError().body("Failed to serialize documents")
                }
            }
        }
        Err(err) => {
            eprintln!("Error fetching documents: {}", err);
            HttpResponse::InternalServerError().body("Failed to fetch documents")
        }
    }
   
}
