create index concurrently if not exists "<index_name>"
  on "<schema_name>"."<table_name>" (
    "<column_name>",
    "<column_name>"
  );
