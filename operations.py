# set the catalog and schema 
def use_catalog_schema(sc, catalog: str, schema: str, env_mode: str = "dev", verbose: bool = False):
    if env_mode == "prd":
        catalog_stmnt = f"""use catalog {catalog};"""
    else:
        catalog_stmnt = f"""use catalog {catalog}_{env_mode};"""
    
    sc.sql(catalog_stmnt)
    sc.sql(f"""use schema {schema};""")
    if verbose:
        return sc.sql("""select current_catalog(), current_schema();""")