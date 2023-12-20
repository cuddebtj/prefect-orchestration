SET ROLE postgres;
create role yahoo_importer with login password 'yahoo_importer';
create role mom_exporter with login password 'mom_exporter';
grant yahoo_importer to postgres;
grant mom_exporter to postgres;
create schema if not exists yahoo_json authorization yahoo_importer;
create schema if not exists yahoo_data authorization yahoo_importer;

create or replace function public.update_modified_column()
returns trigger as $$
    begin
        new.modified_timestamp = CURRENT_TIMESTAMP;
        return new;
    end;
$$ language 'plpgsql';
