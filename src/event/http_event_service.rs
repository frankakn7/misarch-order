use axum::{debug_handler, extract::State, http::StatusCode, Json};
use bson::{doc, Uuid};
use log::info;
use mongodb::{options::UpdateOptions, Collection};
use serde::{Deserialize, Serialize};

use crate::{
    event::order_compensation::compensate_order,
    graphql::{
        model::{
            foreign_types::{
                Coupon, ProductVariant, ProductVariantVersion, ShipmentMethod, TaxRate,
            },
            order::Order,
            user::User,
        },
        query::query_object,
    },
};

use super::order_compensation::OrderCompensation;

/// Data to send to Dapr in order to describe a subscription.
#[derive(Serialize)]
pub struct Pubsub {
    #[serde(rename(serialize = "pubsubName"))]
    pub pubsubname: String,
    pub topic: String,
    pub route: String,
}

/// Reponse data to send to Dapr when receiving an event.
#[derive(Serialize)]
pub struct TopicEventResponse {
    pub status: u8,
}

/// Default status is `0` -> Ok, according to Dapr specs.
impl Default for TopicEventResponse {
    fn default() -> Self {
        Self { status: 0 }
    }
}

/// Relevant part of Dapr event wrapped in a cloud envelope.
#[derive(Deserialize, Debug)]
pub struct Event<T> {
    pub topic: String,
    pub data: T,
}

/// Event data containing a UUID.
#[derive(Deserialize, Debug)]
pub struct UuidEventData {
    pub id: Uuid,
}

/// Event data containing a product variant version.
///
/// Differs from product variant version in the `id` field naming.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ProductVariantVersionEventData {
    /// UUID of product variant version.
    pub id: Uuid,
    /// Price of product variant version.
    pub retail_price: u32,
    /// UUID of tax rate associated with order item.
    pub tax_rate_id: Uuid,
    /// UUID of product variant associated with product variant version.
    pub product_variant_id: Uuid,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TaxRateVersionEventData {
    /// UUID of the tax rate version.
    pub id: Uuid,
    /// Rate of the tax rate version.
    pub rate: f64,
    /// Version number of tax rate.
    pub version: u32,
    /// UUID of tax rate associated with order item.
    pub tax_rate_id: Uuid,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserAddressEventData {
    /// UUID of the user address.
    pub id: Uuid,
    /// UUID of user of user address.
    pub user_id: Uuid,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ShipmentFailedEventData {
    /// UUID of the order of shipment.
    pub order_id: Uuid,
    /// UUIDs of the order items of shipment.
    pub order_item_ids: Vec<Uuid>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ShipmentStatusUpdatedEventData {
    /// UUID of the order of shipment.
    pub order_id: Uuid,
    /// UUIDs of the order items of shipment.
    pub order_item_ids: Vec<Uuid>,
    /// Status of shipment.
    pub status: ShipmentStatus,
}

#[derive(Deserialize, Debug)]
/// Shipment status of order.
pub enum ShipmentStatus {
    Pending,
    InProgress,
    Delivered,
    Failed,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProductVariantEventData {
    /// UUID of the product variant to update.
    pub id: Uuid,
    /// New visibility of product variant to update.
    pub is_publicly_visible: String,
}

/// Service state containing database connections.
#[derive(Clone)]
pub struct HttpEventServiceState {
    pub product_variant_collection: Collection<ProductVariant>,
    pub coupon_collection: Collection<Coupon>,
    pub tax_rate_collection: Collection<TaxRate>,
    pub shipment_method_collection: Collection<ShipmentMethod>,
    pub user_collection: Collection<User>,
    pub order_collection: Collection<Order>,
    pub order_compensation_collection: Collection<OrderCompensation>,
    pub http_client: reqwest::Client,
}

/// HTTP endpoint to list topic subsciptions.
pub async fn list_topic_subscriptions() -> Result<Json<Vec<Pubsub>>, StatusCode> {
    let pubsub_product_variant_version = Pubsub {
        pubsubname: "pubsub".to_string(),
        topic: "catalog/product-variant-version/created".to_string(),
        route: "/on-product-variant-version-creation-event".to_string(),
    };
    let pubsub_product_variant_updated = Pubsub {
        pubsubname: "pubsub".to_string(),
        topic: "catalog/product-variant/updated".to_string(),
        route: "/on-product-variant-updated-event".to_string(),
    };
    let pubsub_coupon = Pubsub {
        pubsubname: "pubsub".to_string(),
        topic: "discount/coupon/created".to_string(),
        route: "/on-id-creation-event".to_string(),
    };
    let pubsub_tax_rate_version = Pubsub {
        pubsubname: "pubsub".to_string(),
        topic: "tax/tax-rate-version/created".to_string(),
        route: "/on-tax-rate-version-creation-event".to_string(),
    };
    let pubsub_shipment_method = Pubsub {
        pubsubname: "pubsub".to_string(),
        topic: "shipment/shipment-method/created".to_string(),
        route: "/on-id-creation-event".to_string(),
    };
    let pubsub_user = Pubsub {
        pubsubname: "pubsub".to_string(),
        topic: "user/user/created".to_string(),
        route: "/on-id-creation-event".to_string(),
    };
    let pubsub_user_address = Pubsub {
        pubsubname: "pubsub".to_string(),
        topic: "address/user-address/created".to_string(),
        route: "/on-user-address-creation-event".to_string(),
    };
    let pubsub_user_address_archived = Pubsub {
        pubsubname: "pubsub".to_string(),
        topic: "address/user-address/archived".to_string(),
        route: "/on-user-address-archived-event".to_string(),
    };
    Ok(Json(vec![
        pubsub_product_variant_updated,
        pubsub_product_variant_version,
        pubsub_coupon,
        pubsub_tax_rate_version,
        pubsub_shipment_method,
        pubsub_user,
        pubsub_user_address,
        pubsub_user_address_archived,
    ]))
}

/// HTTP endpoint to receive UUID creation events.
///
/// Includes all creation events that consist of only UUIDs:
/// - `Coupon`
/// - `ShipmentMethod`
/// - `User`
#[debug_handler(state = HttpEventServiceState)]
pub async fn on_id_creation_event(
    State(state): State<HttpEventServiceState>,
    Json(event): Json<Event<UuidEventData>>,
) -> Result<Json<TopicEventResponse>, StatusCode> {
    info!("{:?}", event);

    match event.topic.as_str() {
        "discount/coupon/created" => {
            create_in_mongodb(&state.coupon_collection, event.data.id).await?
        }
        "shipment/shipment-method/created" => {
            create_in_mongodb(&state.shipment_method_collection, event.data.id).await?
        }
        "user/user/created" => create_in_mongodb(&state.user_collection, event.data.id).await?,
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    Ok(Json(TopicEventResponse::default()))
}

/// HTTP endpoint to receive product variant version creation events.
#[debug_handler(state = HttpEventServiceState)]
pub async fn on_product_variant_version_creation_event(
    State(state): State<HttpEventServiceState>,
    Json(event): Json<Event<ProductVariantVersionEventData>>,
) -> Result<Json<TopicEventResponse>, StatusCode> {
    info!("{:?}", event);
    match event.topic.as_str() {
        "catalog/product-variant-version/created" => {
            create_or_update_product_variant_in_mongodb(
                &state.product_variant_collection,
                event.data,
            )
            .await?;
        }
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    Ok(Json(TopicEventResponse::default()))
}

/// HTTP endpoint to receive product variant update events.
#[debug_handler(state = HttpEventServiceState)]
pub async fn on_product_variant_update_event(
    State(state): State<HttpEventServiceState>,
    Json(event): Json<Event<UpdateProductVariantEventData>>,
) -> Result<Json<TopicEventResponse>, StatusCode> {
    info!("{:?}", event);

    match event.topic.as_str() {
        "catalog/product-variant/updated" => {
            update_product_variant_visibility_in_mongodb(
                &state.product_variant_collection,
                event.data,
            )
            .await?
        }
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    Ok(Json(TopicEventResponse::default()))
}

/// HTTP endpoint to receive tax rate version creation events.
///
/// * `state` - Service state containing database connections.
/// * `event` - Event handled by endpoint.
#[debug_handler(state = HttpEventServiceState)]
pub async fn on_tax_rate_version_creation_event(
    State(state): State<HttpEventServiceState>,
    Json(event): Json<Event<TaxRateVersionEventData>>,
) -> Result<Json<TopicEventResponse>, StatusCode> {
    info!("{:?}", event);

    let tax_rate = TaxRate::from(event.data);
    match event.topic.as_str() {
        "tax/tax-rate-version/created" => {
            create_or_update_tax_rate_in_mongodb(&state.tax_rate_collection, tax_rate).await?
        }
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    Ok(Json(TopicEventResponse::default()))
}

/// HTTP endpoint to receive user address creation events.
///
/// * `state` - Service state containing database connections.
/// * `event` - Event handled by endpoint.
#[debug_handler(state = HttpEventServiceState)]
pub async fn on_user_address_creation_event(
    State(state): State<HttpEventServiceState>,
    Json(event): Json<Event<UserAddressEventData>>,
) -> Result<Json<TopicEventResponse>, StatusCode> {
    info!("{:?}", event);

    match event.topic.as_str() {
        "address/user-address/created" => {
            insert_user_address_in_mongodb(&state.user_collection, event.data).await?
        }
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    Ok(Json(TopicEventResponse::default()))
}

/// HTTP endpoint to receive user address archive events.
///
/// * `state` - Service state containing database connections.
/// * `event` - Event handled by endpoint.
#[debug_handler(state = HttpEventServiceState)]
pub async fn on_user_address_archived_event(
    State(state): State<HttpEventServiceState>,
    Json(event): Json<Event<UserAddressEventData>>,
) -> Result<Json<TopicEventResponse>, StatusCode> {
    info!("{:?}", event);

    match event.topic.as_str() {
        "address/user-address/archived" => {
            remove_user_address_in_mongodb(&state.user_collection, event.data).await?
        }
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    Ok(Json(TopicEventResponse::default()))
}

/// HTTP endpoint to receive shipment creation events.
///
/// * `state` - Service state containing database connections.
/// * `event` - Event handled by endpoint.
#[debug_handler(state = HttpEventServiceState)]
pub async fn on_shipment_creation_failed_event(
    State(state): State<HttpEventServiceState>,
    Json(event): Json<Event<ShipmentFailedEventData>>,
) -> Result<Json<TopicEventResponse>, StatusCode> {
    info!("{:?}", event);

    match event.topic.as_str() {
        "shipment/shipment/creation-failed" => compensate_order(
            &state.order_collection,
            &state.order_compensation_collection,
            event.data,
            &state.http_client,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    Ok(Json(TopicEventResponse::default()))
}

/// Create or update product variant in MongoDB.
///
/// * `collection` - MongoDB collection to create or update product variant in.
/// * `product_variant_version_event_data` - Product variant version event data containg product variant version to create or update.
pub async fn create_or_update_product_variant_in_mongodb(
    collection: &Collection<ProductVariant>,
    product_variant_version_event_data: ProductVariantVersionEventData,
) -> Result<(), StatusCode> {
    match query_object(
        collection,
        product_variant_version_event_data.product_variant_id,
    )
    .await
    {
        Ok(product_variant) => {
            update_product_variant_in_mongodb(
                product_variant_version_event_data,
                collection,
                product_variant,
            )
            .await
        }
        Err(e) => {
            log::info!("Error {:?}", e);
            create_product_variant_in_mongodb(product_variant_version_event_data, collection).await
        }
    }
}

/// Update product variant in MongoDB.
///
/// * `product_variant_version_event_data` - Product variant version event data containg new product variant version.
/// * `collection` - MongoDB collection to update product variant in.
/// * `product_variant` - Product variant to update.
async fn update_product_variant_in_mongodb(
    product_variant_version_event_data: ProductVariantVersionEventData,
    collection: &Collection<ProductVariant>,
    product_variant: ProductVariant,
) -> Result<(), StatusCode> {
    let product_variant_version = ProductVariantVersion::from(product_variant_version_event_data);
    log::info!("{:?}", product_variant_version);
    match collection
        .update_one(
            doc! {"_id": product_variant._id},
            doc! {"$set": {"current_version": product_variant_version}},
            None,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Create product variant in MongoDB.
///
/// * `product_variant_version_event_data` - Product variant version event data to create product variant with.
/// * `collection` - MongoDB collection to create product variant in.
async fn create_product_variant_in_mongodb(
    product_variant_version_event_data: ProductVariantVersionEventData,
    collection: &Collection<ProductVariant>,
) -> Result<(), StatusCode> {
    let product_variant = ProductVariant::from(product_variant_version_event_data);
    match collection.insert_one(product_variant, None).await {
        Ok(_) => Ok(()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Create or update tax rate in MongoDB.
///
/// * `collection` - MongoDB collection to create or update tax rate in.
/// * `tax_rate` - Tax rate to create or update.
pub async fn create_or_update_tax_rate_in_mongodb(
    collection: &Collection<TaxRate>,
    tax_rate: TaxRate,
) -> Result<(), StatusCode> {
    let update_options = UpdateOptions::builder().upsert(true).build();
    match collection
        .update_one(
            doc! {"_id": tax_rate._id },
            doc! {"$set": {"_id": tax_rate._id, "current_version": tax_rate.current_version}},
            update_options,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Inserts user address in MongoDB.
///
/// * `collection` - MongoDB collection to insert user address in.
/// * `user_address_event_data` - User address event data containing user address to insert.
pub async fn insert_user_address_in_mongodb(
    collection: &Collection<User>,
    user_address_event_data: UserAddressEventData,
) -> Result<(), StatusCode> {
    match collection
        .update_one(
            doc! {"_id": user_address_event_data.user_id },
            doc! {"$push": {"user_address_ids": user_address_event_data.id }},
            None,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Remove user address from MongoDB.
///
/// * `collection` - MongoDB collection remove user address from.
/// * `user_address_event_data` - User address event data containing user address to remove.
pub async fn remove_user_address_in_mongodb(
    collection: &Collection<User>,
    user_address_event_data: UserAddressEventData,
) -> Result<(), StatusCode> {
    match collection
        .update_one(
            doc! {"_id": user_address_event_data.user_id },
            doc! {"$pull": {"user_address_ids": user_address_event_data.id }},
            None,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Updates visibility of product variant in MongoDB.
///
/// * `collection` - MongoDB collection to update the product visibility in.
/// * `update_product_variant_event_data` - Update product variant event data containing new product visibility.
async fn update_product_variant_visibility_in_mongodb(
    collection: &Collection<ProductVariant>,
    update_product_variant_event_data: UpdateProductVariantEventData,
) -> Result<(), StatusCode> {
    match collection
        .update_one(
            doc! {"_id": update_product_variant_event_data.id },
            doc! {"$set": {"is_publicly_visible": update_product_variant_event_data.is_publicly_visible }},
            None,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Create a new object: `T` in MongoDB.
///
/// * `collection` - MongoDB collection to add newly created object to.
/// * `id` - UUID of newly created object.
pub async fn create_in_mongodb<T: Serialize + From<Uuid>>(
    collection: &Collection<T>,
    id: Uuid,
) -> Result<(), StatusCode> {
    let object = T::from(id);
    match collection.insert_one(object, None).await {
        Ok(_) => Ok(()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
