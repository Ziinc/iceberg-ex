//! `Iceberg.Catalog`: connecting, namespaces and tables.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::memory::MemoryCatalogBuilder;
use iceberg::spec::Schema;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use rustler::{Atom, Env, NifResult, ResourceArc, Term};

use crate::error::Error;
use crate::table::TableResource;
use crate::{atoms, spawn, term};

pub struct CatalogResource(Arc<dyn Catalog>);

#[rustler::resource_impl]
impl rustler::Resource for CatalogResource {}

type CatalogArc = ResourceArc<CatalogResource>;

/// Files are read and written through OpenDAL, by the location's scheme
/// (`s3://`, `file://`, `memory://`), configured from the catalog's `s3.*` properties.
async fn connect(
    kind: &str,
    name: &str,
    props: HashMap<String, String>,
) -> Result<Arc<dyn Catalog>, Error> {
    let storage = Arc::new(OpenDalResolvingStorageFactory::new());
    Ok(match kind {
        "rest" => Arc::new(
            RestCatalogBuilder::default()
                .with_storage_factory(storage)
                .load(name, props)
                .await?,
        ),
        "s3tables" => Arc::new(
            S3TablesCatalogBuilder::default()
                .with_storage_factory(storage)
                .load(name, props)
                .await?,
        ),
        "memory" => Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(storage)
                .load(name, props)
                .await?,
        ),
        other => {
            return Err(Error::new(
                "feature_unsupported",
                format!("unknown catalog kind {other}"),
            ))
        }
    })
}

fn ns(levels: Vec<String>) -> Result<NamespaceIdent, Error> {
    Ok(NamespaceIdent::from_vec(levels)?)
}

fn ident(levels: Vec<String>, name: String) -> Result<TableIdent, Error> {
    Ok(TableIdent::new(ns(levels)?, name))
}

#[rustler::nif]
fn catalog_new(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    kind: String,
    name: String,
    props: HashMap<String, String>,
) -> Atom {
    spawn(env, r, timeout, async move {
        Ok(ResourceArc::new(CatalogResource(
            connect(&kind, &name, props).await?,
        )))
    })
}

#[rustler::nif]
fn list_namespaces(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    parent: Option<Vec<String>>,
) -> Atom {
    spawn(env, r, timeout, async move {
        let parent = parent.map(ns).transpose()?;
        let list = c.0.list_namespaces(parent.as_ref()).await?;
        Ok(list
            .into_iter()
            .map(NamespaceIdent::inner)
            .collect::<Vec<_>>())
    })
}

#[rustler::nif]
fn create_namespace(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
    props: HashMap<String, String>,
) -> Atom {
    spawn(env, r, timeout, async move {
        c.0.create_namespace(&ns(levels)?, props).await?;
        Ok(atoms::ok())
    })
}

#[rustler::nif]
fn get_namespace(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
) -> Atom {
    spawn(env, r, timeout, async move {
        Ok(c.0.get_namespace(&ns(levels)?).await?.properties().clone())
    })
}

#[rustler::nif]
fn namespace_exists(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
) -> Atom {
    spawn(env, r, timeout, async move {
        Ok(c.0.namespace_exists(&ns(levels)?).await?)
    })
}

#[rustler::nif]
fn drop_namespace(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
) -> Atom {
    spawn(env, r, timeout, async move {
        c.0.drop_namespace(&ns(levels)?).await?;
        Ok(atoms::ok())
    })
}

#[rustler::nif]
fn list_tables(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
) -> Atom {
    spawn(env, r, timeout, async move {
        let tables = c.0.list_tables(&ns(levels)?).await?;
        Ok(tables
            .into_iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>())
    })
}

/// `partition` holds `{source column, partition field name, transform}`.
/// Creates an unpartitioned table; `schema` is in the spec's JSON shape.
#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn create_table(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
    name: String,
    schema: Term,
    props: HashMap<String, String>,
) -> NifResult<Atom> {
    let schema = term::to_json(schema)?;
    Ok(spawn(env, r, timeout, async move {
        let schema: Schema = serde_json::from_value(schema)
            .map_err(|e| Error::invalid(format!("invalid schema: {e}")))?;
        let creation = TableCreation::builder()
            .name(name)
            .schema(schema)
            .properties(props)
            .build();
        let table = c.0.create_table(&ns(levels)?, creation).await?;
        Ok(TableResource::new(c.0.clone(), table))
    }))
}

#[rustler::nif]
fn load_table(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
    name: String,
) -> Atom {
    spawn(env, r, timeout, async move {
        let table = c.0.load_table(&ident(levels, name)?).await?;
        Ok(TableResource::new(c.0.clone(), table))
    })
}

#[rustler::nif]
fn table_exists(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
    name: String,
) -> Atom {
    spawn(env, r, timeout, async move {
        Ok(c.0.table_exists(&ident(levels, name)?).await?)
    })
}

#[rustler::nif]
fn drop_table(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    levels: Vec<String>,
    name: String,
) -> Atom {
    spawn(env, r, timeout, async move {
        c.0.drop_table(&ident(levels, name)?).await?;
        Ok(atoms::ok())
    })
}

#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn rename_table(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    c: CatalogArc,
    from_levels: Vec<String>,
    from: String,
    to_levels: Vec<String>,
    to: String,
) -> Atom {
    spawn(env, r, timeout, async move {
        c.0.rename_table(&ident(from_levels, from)?, &ident(to_levels, to)?)
            .await?;
        Ok(atoms::ok())
    })
}
