grant all privileges
    on all tables in schema public, yahoo_data, yahoo_json
    to yahoo_importer;
grant all privileges
    on all sequences in schema public, yahoo_data, yahoo_json
    to yahoo_importer;
grant all privileges
    on all functions in schema public, yahoo_data, yahoo_json
    to yahoo_importer;
grant all privileges
    on all procedures in schema public, yahoo_data, yahoo_json
    to yahoo_importer;
grant all privileges
    on all routines in schema public, yahoo_data, yahoo_json
    to yahoo_importer;
grant all privileges
    on schema public, yahoo_data, yahoo_json
    to yahoo_importer;

grant all privileges
    on all tables in schema public
    to mom_exporter;
grant all privileges
    on all sequences in schema public
    to mom_exporter;
grant all privileges
    on all functions in schema public
    to mom_exporter;
grant all privileges
    on all procedures in schema public
    to mom_exporter;
grant all privileges
    on all routines in schema public
    to mom_exporter;
grant all privileges
    on schema public
    to mom_exporter;

grant all privileges on schema public to yahoo_importer;
grant all privileges on schema yahoo_json to yahoo_importer;
grant all privileges on schema yahoo_data to yahoo_importer;

grant all privileges on schema public to mom_exporter;
