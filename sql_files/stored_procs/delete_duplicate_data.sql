-- DROP PROCEDURE yahoo_data.delete_duplicate_data();
CREATE OR REPLACE PROCEDURE yahoo_data.delete_duplicate_data(schema_name text, table_name text)
  AS $$
  DECLARE
    col_name text;
    grp_name text;
    del_name text;
    column_names record;
    delete_string text;
    query_string text DEFAULT 'SELECT ';
    group_by_string text DEFAULT ' GROUP BY ';

  BEGIN
    DROP TABLE IF EXISTS records_to_keep;

    FOR column_names in
      EXECUTE 'SELECT column_name FROM information_schema.columns '
        || 'WHERE table_schema = $1 AND table_name = $2'
        USING schema_name, table_name

      LOOP
        IF column_names.column_name = 'inserted_timestamp' THEN
          col_name := 'min('|| quote_ident(column_names.column_name) || ') AS inserted_timestamp, ';
          grp_name := '';
          del_name := '';
        ELSE
          col_name := quote_ident(column_names.column_name) || ', ';
          grp_name := quote_ident(column_names.column_name) || ', ';
          del_name := ' AND coalesce(tgt.'
            || quote_ident(column_names.column_name)
            || ', '''') = coalesce(src.'
            || quote_ident(column_names.column_name)
            || ', '''')';
        END IF;

        query_string := query_string || col_name;
        group_by_string := group_by_string || grp_name;
        delete_string := delete_string || del_name;

    END LOOP;

    query_string := 'CREATE TEMP TABLE records_to_keep AS '
      || left(query_string, -2)
      || ' FROM '
      || quote_ident(schema_name)
      || '.'
      || quote_ident(table_name)
      || left(group_by_string, -2);

    EXECUTE query_string;

    delete_string := 'DELETE FROM '
      || quote_ident(schema_name)
      || '.'
      || quote_ident(table_name)
      || ' AS tgt USING records_to_keep AS src WHERE tgt.inserted_timestamp != src.inserted_timestamp'
      || delete_string;

    EXECUTE delete_string;

  END;
$$ LANGUAGE plpgsql;
