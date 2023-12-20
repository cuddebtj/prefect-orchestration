alter default privileges
    for role mom_exporter
    grant all privileges on schemas to mom_exporter;
alter default privileges
    for role mom_exporter
    in schema public
    grant all privileges on tables to mom_exporter;
alter default privileges
    for role mom_exporter
    in schema public
    grant all privileges on sequences to mom_exporter;
alter default privileges
    for role mom_exporter
    in schema public
    grant all privileges on functions to mom_exporter;
alter default privileges
    for role mom_exporter
    in schema public
    grant all privileges on routines to mom_exporter;
alter default privileges
    for role mom_exporter
    in schema public
    grant all privileges on types to mom_exporter;

alter default privileges
    for role yahoo_importer
    grant all privileges on schemas to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema public
    grant all privileges on tables to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema public
    grant all privileges on sequences to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema public
    grant all privileges on functions to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema public
    grant all privileges on routines to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema public
    grant all privileges on types to yahoo_importer;

alter default privileges
    for role yahoo_importer
    in schema yahoo_json
    grant all privileges on tables to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema yahoo_json
    grant all privileges on sequences to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema yahoo_json
    grant all privileges on functions to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema yahoo_json
    grant all privileges on routines to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema yahoo_json
    grant all privileges on types to yahoo_importer;

alter default privileges
    for role yahoo_importer
    in schema yahoo_data
    grant all privileges on tables to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema yahoo_data
    grant all privileges on sequences to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema yahoo_data
    grant all privileges on functions to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema yahoo_data
    grant all privileges on routines to yahoo_importer;
alter default privileges
    for role yahoo_importer
    in schema yahoo_data
    grant all privileges on types to yahoo_importer;
