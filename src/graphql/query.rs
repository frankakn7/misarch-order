use std::{any::type_name, collections::HashMap};

use async_graphql::{Context, Error, Object, Result};

use bson::Uuid;
use futures::TryStreamExt;
use mongodb::{bson::doc, Collection, Database};
use serde::Deserialize;

use tracing::instrument;

use crate::authorization::authorize_user;

use super::model::{order::Order, order_item::OrderItem, user::User};

/// Describes GraphQL order queries.
pub struct Query;

#[Object]
impl Query {
    /// Entity resolver for user of specific UUID.
    #[graphql(entity)]
    async fn user_entity_resolver<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(desc = "UUID of user to retrieve.")] id: Uuid,
    ) -> Result<User> {
        let db_client = ctx.data::<Database>()?;
        let collection: Collection<User> = db_client.collection::<User>("users");
        query_object(&collection, id).await
    }

    /// Retrieves order of specific UUID.
    async fn order<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(desc = "UUID of order to retrieve.")] id: Uuid,
    ) -> Result<Order> {
        let db_client = ctx.data::<Database>()?;
        let collection: Collection<Order> = db_client.collection::<Order>("orders");
        let order = query_object(&collection, id).await?;
        authorize_user(&ctx, Some(order.user._id))?;
        Ok(order)
    }

    /// Entity resolver for order of specific UUID.
    #[graphql(entity)]
    async fn order_entity_resolver<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(key, desc = "UUID of order to retrieve.")] id: Uuid,
    ) -> Result<Order> {
        let db_client = ctx.data::<Database>()?;
        let collection: Collection<Order> = db_client.collection::<Order>("orders");
        let order = query_object(&collection, id).await?;
        Ok(order)
    }

    /// Retrieves order_item of specific UUID.
    async fn order_item<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(desc = "UUID of order_item to retrieve.")] id: Uuid,
    ) -> Result<OrderItem> {
        let db_client = ctx.data::<Database>()?;
        let order_collection: Collection<Order> = db_client.collection::<Order>("orders");
        let order_item_collection: Collection<OrderItem> =
            db_client.collection::<OrderItem>("order_items");
        let order_item = query_object(&order_item_collection, id).await?;
        let user = query_user_from_order_item_id(&order_collection, id).await?;
        authorize_user(&ctx, Some(user._id))?;
        Ok(order_item)
    }

    /// Entity resolver for order_item of specific UUID.
    #[graphql(entity)]
    async fn order_item_entity_resolver<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(key, desc = "UUID of order_item to retrieve.")] id: Uuid,
    ) -> Result<OrderItem> {
        let db_client = ctx.data::<Database>()?;
        let collection: Collection<OrderItem> = db_client.collection::<OrderItem>("order_items");
        let order_item = query_object(&collection, id).await?;
        Ok(order_item)
    }
}

/// Queries a user from an order item UUID.
///
/// * `collection` - MongoDB collection of orders to retrieve user of order item from.
/// * `id` - UUID of order item.
async fn query_user_from_order_item_id(collection: &Collection<Order>, id: Uuid) -> Result<User> {
    match collection
        .find_one(doc! {"internal_order_items._id": id }, None)
        .await
    {
        Ok(maybe_order) => match maybe_order {
            Some(order) => Ok(order.user),
            None => {
                let message = format!("OrderItem with UUID: `{}` not found.", id);
                Err(Error::new(message))
            }
        },
        Err(_) => {
            let message = format!("OrderItem with UUID: `{}` not found.", id);
            Err(Error::new(message))
        }
    }
}

/// Shared function to query an object: `T` from a MongoDB collection of object: `T`.
///
/// * `connection` - MongoDB database connection.
/// * `id` - UUID of object.
#[instrument(skip(collection), fields(id = %id))]
pub async fn query_object<T: for<'a> Deserialize<'a> + Unpin + Send + Sync>(
    collection: &Collection<T>,
    id: Uuid,
) -> Result<T> {
    match collection.find_one(doc! {"_id": id }, None).await {
        Ok(maybe_object) => match maybe_object {
            Some(object) => Ok(object),
            None => {
                let message = format!("{} with UUID: `{}` not found.", type_name::<T>(), id);
                Err(Error::new(message))
            }
        },
        Err(_) => {
            let message = format!("{} with UUID: `{}` not found.", type_name::<T>(), id);
            Err(Error::new(message))
        }
    }
}

/// Shared function to query objects: `T` from a MongoDB collection of object: `T`.
///
/// * `connection` - MongoDB database connection.
/// * `ids` - UUIDs of objects.
#[instrument(skip(collection, object_ids), fields(count = object_ids.len()))]
pub async fn query_objects<T: for<'a> Deserialize<'a> + Unpin + Send + Sync + Clone>(
    collection: &Collection<T>,
    object_ids: &Vec<Uuid>,
) -> Result<HashMap<Uuid, T>>
where
    Uuid: From<T>,
{
    match collection
        .find(doc! {"_id": { "$in": &object_ids } }, None)
        .await
    {
        Ok(cursor) => {
            let objects: HashMap<Uuid, T> = cursor
                .try_fold(HashMap::new(), |mut map, result| async move {
                    let id = Uuid::from(result.clone());
                    map.insert(id, result);
                    Ok(map)
                })
                .await?;
            Ok(objects)
        }
        Err(_) => {
            let message = format!(
                "{} with UUIDs: `{:?}` not found.",
                type_name::<T>(),
                object_ids
            );
            Err(Error::new(message))
        }
    }
}
