use async_graphql::{Error, Result};
use bson::{doc, DateTime, Uuid};
use futures::TryStreamExt;
use mongodb::Collection;
use serde::{Deserialize, Serialize};

use crate::graphql::{model::order::Order, mutation::validate_object, query::query_object};

use super::{
    http_event_service::ShipmentFailedEventData,
    model::order_compensation_dto::OrderCompensationDTO,
};

/// Models an order compensation that is sent as an event and logged in MongoDB.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderCompensation {
    /// Order compensation UUID.
    pub _id: Uuid,
    /// UUID of the order.
    pub order_id: Uuid,
    /// UUIDs of the order items of shipment.
    pub order_item_ids: Vec<Uuid>,
    /// Timestamp when compensation was triggered.
    pub triggered_at: DateTime,
    /// Amount of order compensation.
    pub amount_to_compensate: u64,
}

/// Responsible for compensating a shipment based on a failed shipment event. Saves compensation in MongoDB.
///
/// * `order_collection` - MongoDB collection to validate order with.
/// * `order_compensation_collection` - MongoDB collection to compensate order in.
/// * `shipment_failed_event_data` - Event data of failed shipment event containing UUID of order to compensate.
pub async fn compensate_order(
    order_collection: &Collection<Order>,
    order_compensation_collection: &Collection<OrderCompensation>,
    shipment_failed_event_data: ShipmentFailedEventData,
    http_client: &reqwest::Client,
) -> Result<()> {
    validate_object(&order_collection, shipment_failed_event_data.order_id).await?;
    verify_items_uncompensated(
        &order_compensation_collection,
        &shipment_failed_event_data.order_item_ids,
    )
    .await?;
    let amount_to_compensate =
        calculate_amount_to_compensate(&order_collection, &shipment_failed_event_data).await?;
    let order_compensation = OrderCompensation {
        _id: Uuid::new(),
        order_id: shipment_failed_event_data.order_id,
        order_item_ids: shipment_failed_event_data.order_item_ids,
        triggered_at: DateTime::now(),
        amount_to_compensate,
    };
    insert_order_compensation_in_mongodb(&order_compensation_collection, &order_compensation)
        .await?;
    send_order_compensation_event(order_compensation, http_client).await
}

/// Calculates the amount that the compensation event should compensate. Based on the failed shipment event.
///
/// * `order_collection` - MongoDB collection containing order to calculate compensatable amount from.
/// * `shipment_failed_event_data` - Event data of failed shipment event containing UUID of order to calculate compensatable amount for.
async fn calculate_amount_to_compensate(
    order_collection: &Collection<Order>,
    shipment_failed_event_data: &ShipmentFailedEventData,
) -> Result<u64> {
    let order = query_object(&order_collection, shipment_failed_event_data.order_id).await?;
    let compensatable_amounts: Vec<u64> = order
        .internal_order_items
        .iter()
        .filter(|order_item| {
            shipment_failed_event_data
                .order_item_ids
                .contains(&order_item._id)
        })
        .map(|order_item| order_item.compensatable_amount)
        .collect();
    let amount_to_compensate = compensatable_amounts.iter().sum();
    Ok(amount_to_compensate)
}

/// Verifies that all of the items are uncompensated, otherwise returns an error.
///
/// * `order_compensation_collection` - MongoDB collection of order compensations.
/// * `order_item_ids` - UUIDs of order items to verify as uncompensated.
async fn verify_items_uncompensated(
    order_compensation_collection: &Collection<OrderCompensation>,
    order_item_ids: &Vec<Uuid>,
) -> Result<()> {
    let query = doc! {"order_item_ids": {"$not": {"$elemMatch": {"$in": order_item_ids}}}};
    let message = format!(
        "Order items of UUIDs: `{:?}` could not be verfied.",
        order_item_ids
    );
    match order_compensation_collection.find(query, None).await {
        Ok(cursor) => {
            let objects: Vec<OrderCompensation> = cursor.try_collect().await?;
            match objects.len() {
                0 => Ok(()),
                _ => Err(Error::new(message)),
            }
        }
        Err(_) => Err(Error::new(message)),
    }
}

/// Inserts order compenstation in MongoDB.
///
/// * `collection` - MongoDB collection to insert order compensation in.
/// * `order_compensation` - Order compensation to insert.
async fn insert_order_compensation_in_mongodb(
    collection: &Collection<OrderCompensation>,
    order_compensation: &OrderCompensation,
) -> Result<()> {
    match collection.insert_one(order_compensation, None).await {
        Ok(_) => Ok(()),
        Err(_) => Err(Error::new("Adding order compensation failed in MongoDB.")),
    }
}

/// Sends an `order/order/compensate` created event containing the amount to compensate.
///
/// * `order_compensation` - Order compensation to create event with.
async fn send_order_compensation_event(
    order_compensation: OrderCompensation,
    http_client: &reqwest::Client,
) -> Result<()> {
    let order_compensation_dto = OrderCompensationDTO::from(order_compensation);
    http_client
        .post("http://localhost:3500/v1.0/publish/pubsub/order/order-compensation/created")
        .json(&order_compensation_dto)
        .send()
        .await?;
    Ok(())
}
