create role yahoo_importer with login password 'yahoo_importer' inherit 'service_role';
create role mom_exporter with login password 'mom_exporter' inherit 'service_role';
create schema if not exists yahoo_json authorization yahoo_importer;
create schema if not exists yahoo_data authorization yahoo_importer;

create or replace function public.update_modified_column()
returns trigger as $$
    begin
        new.modified_timestamp = NOW();
        return new;
    end;
$$ language 'plpgsql';

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
