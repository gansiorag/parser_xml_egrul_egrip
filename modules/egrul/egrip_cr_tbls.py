"""
Создание всех тестовых таблиц в тестовой схеме для
сохранения данных из СМЭВ по ЕГРИП
Author: Gansior Alexander, gansior@gansior.ru, +79173383804
Starting 2025/08/04
Ending 2025/08/
"""
import sys

# Добавляем путь для подключения modules
new_path = "/opt/airflow2/"
sys.path.append(new_path)
from modules.egrul.com_f import conn_base, get_logger


logger = get_logger()


def all_tables_egrip(var_out, var_schem: str):
    """Создание всех таблиц ЕГРИП
       структура указывается в словаре запросов.
       ключ это название таблицы.

    Args:
        var_out (str): переменная связи с соответсвующей базой
        var_schem (str): название схемы где будут слздаваться 
        таблицы
    """

    tables = {
        "h_individual_entrepreneur_egrip_main": f"""CREATE TABLE  IF NOT EXISTS {var_schem}.h_individual_entrepreneur_egrip_main (
                statement_dt date NULL,
                ogrnip_dt date NULL,
                innfl bpchar(12) NULL,
                vid_ip_code int2 NULL,
                vid_ip_name text NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                CONSTRAINT h_individual_entrepreneur_egrip_main_pkey PRIMARY KEY (individual_entrepreneur_pk)
                );""",
        "s_individual_entrepreneur_address_fias_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_address_fias_info (
                svadrmj_adrmjfias_idnom text NULL,
                svadrmj_adrmjfias_indeks text NULL,
                svadrmj_adrmjfias_region bpchar(2) NULL,
                svadrmj_adrmjfias_naimregion text NULL,
                svadrmj_adrmjfias_municipraion_vidkod text NULL,
                svadrmj_adrmjfias_municipraion_naim text NULL,
                svadrmj_adrmjfias_gorodselposelen_vidkod text NULL,
                svadrmj_adrmjfias_gorodselposelen_naim text NULL,
                svadrmj_adrmjfias_naselenpunkt_vid text NULL,
                svadrmj_adrmjfias_naselenpunkt_naim text NULL,
                svadrmj_adrmjfias_elplanstruktur_tip text NULL,
                svadrmj_adrmjfias_elplanstruktur_naim text NULL,
                svadrmj_adrmjfias_eluldorseti_tip text NULL,
                svadrmj_adrmjfias_eluldorseti_naim text NULL,
                svadrmj_adrmjfias_zdanie_tip text NULL,
                svadrmj_adrmjfias_zdanie_nomer text NULL,
                svadrmj_adrmjfias_pomeszdania_tip text NULL,
                svadrmj_adrmjfias_pomeszdania_nomer text NULL,
                svadrmj_adrmjfias_pomeskvartiri_tip text NULL,
                svadrmj_adrmjfias_pomeskvartiri_nomer text NULL,
                svadrmj_grnipdata_grnip text NULL,
                svadrmj_grnipdata_valid_from date NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                CONSTRAINT s_individual_entrepreneur_address_fias_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_address_fias_info ADD CONSTRAINT s_individual_entrepreneur_address_fias_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_address_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_address_info (
                svadrmj_adresrf_kodregion bpchar(2) NULL,
                svadrmj_adresrf_gorod_naimgorod text NULL,
                svadrmj_adresrf_gorod_tipgorod text NULL,
                svadrmj_adresrf_naselpunkt_naimnaselpunkt text NULL,
                svadrmj_adresrf_naselpunkt_tipnaselpunkt text NULL,
                svadrmj_adresrf_rajon_naimrajon text NULL,
                svadrmj_adresrf_rajon_tiprajon text NULL,
                svadrmj_adresrf_region_naimregion text NULL,
                svadrmj_adresrf_region_tipregion text NULL,
                svadrmj_grnipdata_grnip text NULL,
                svadrmj_grnipdata_valid_from date NULL,
                hierarchy_fulltext text NULL,
                fias_guid text NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                CONSTRAINT s_individual_entrepreneur_address_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_address_info ADD CONSTRAINT s_individual_entrepreneur_address_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_citizenship_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_citizenship_info (
                svgrajd_vidgrajd int2 NULL,
                svgrajd_naimstran text NULL,
                svgrajd_oksm bpchar(3) NULL,
                svgrajd_grnipdata_grnip text NULL,
                svgrajd_grnipdata_valid_from date NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_citizenship_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_citizenship_info ADD CONSTRAINT s_individual_entrepreneur_citizenship_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_egrip_history": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_egrip_history (
                svzapegrip_grnip text NULL,
                svzapegrip_datazap date NULL,
                svzapegrip_idzap int8 NULL,
                svzapegrip_vidzap_kodspvz int4 NULL,
                svzapegrip_vidzap_naimvidzap text NULL,
                svzapegrip_grnipdatanedpred_grnip text NULL,
                svzapegrip_grnipdatanedpred_datazap date NULL,
                svzapegrip_grnipdatanedpred_idzap int8 NULL,
                svzapegrip_svregorg_kodno int4 NULL,
                svzapegrip_svregorg_naimno text NULL,
                svzapegrip_svsvid_datavydsvid text NULL,
                svzapegrip_svsvid_nomer text NULL,
                svzapegrip_svsvid_seria text NULL,
                svzapegrip_svstatuszap_grnipdataned_grnip text NULL,
                svzapegrip_svstatuszap_grnipdataned_datazap date NULL,
                svzapegrip_svstatuszap_grnipdataned_idzap int8 NULL,
                svzapegrip_svedpreddok_datadok text NULL,
                svzapegrip_svedpreddok_naimdok text NULL,
                svzapegrip_svedpreddok_nomdok text NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_egrip_history_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            CREATE INDEX s_individual_entrepreneur_egrip_history_ogrnip_idx ON {var_schem}.s_individual_entrepreneur_egrip_history USING btree (ogrnip);
            ALTER TABLE {var_schem}.s_individual_entrepreneur_egrip_history ADD CONSTRAINT s_individual_entrepreneur_egrip_history_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_email_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_email_info (
                svadrelpocty_email text NULL,
                svadrelpocty_grnipdata_grnip text NULL,
                svadrelpocty_grnipdata_valid_from date NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_email_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_email_info ADD CONSTRAINT s_individual_entrepreneur_email_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_individual_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_individual_info (
                svfl_fiolat_familia text NULL,
                svfl_fiolat_ima text NULL,
                svfl_fiolat_otcestvo text NULL,
                svfl_fiorus_familia text NULL,
                svfl_fiorus_ima text NULL,
                svfl_fiorus_otcestvo text NULL,
                svfl_pol text NULL,
                svfl_grnipdata_grnip text NULL,
                svfl_grnipdata_valid_from date NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                statement_dt date NULL,
                hash_diff bpchar(32) NOT NULL,
                CONSTRAINT s_individual_entrepreneur_individual_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_individual_info ADD CONSTRAINT s_individual_entrepreneur_individual_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_is_liquidated_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_is_liquidated_info (
                svprekras_grnipdata_grnip text NULL,
                svprekras_grnipdata_valid_from date NULL,
                svprekras_svstatus_dataprekras date NULL,
                svprekras_svstatus_kodstatus int4 NULL,
                svprekras_svstatus_naimstatus text NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_is_liquidated_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_is_liquidated_info ADD CONSTRAINT s_individual_entrepreneur_is_liquidated_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_licenses_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_licenses_info (
                svlizenzia_dataliz date NULL,
                svlizenzia_datanacliz date NULL,
                svlizenzia_nomliz text NULL,
                svlizenzia_grnipdata_grnip text NULL,
                svlizenzia_grnipdata_valid_from date NULL,
                svlizenzia_lizorgvydliz text NULL,
                svlizenzia_naimlizviddeat text NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_licenses_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_licenses_info ADD CONSTRAINT s_individual_entrepreneur_licenses_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_main_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_main_info (
                innfl bpchar(12) NULL,
                vid_ip_code int2 NULL,
                vid_ip_name text NULL,
                ogrnip bpchar(15) NULL,
                statement_dt date NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                CONSTRAINT s_individual_entrepreneur_main_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_main_info ADD CONSTRAINT s_individual_entrepreneur_main_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_okveds_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_okveds_info (
                svokved_svokveddop_kodokved text NULL,
                svokved_svokveddop_naimokved text NULL,
                svokved_svokveddop_prversokved int2 NULL,
                svokved_svokveddop_grnipdata_grnip text NULL,
                svokved_svokveddop_grnipdata_valid_from date NULL,
                svokved_svokvedosn_kodokved text NULL,
                svokved_svokvedosn_naimokved text NULL,
                svokved_svokvedosn_prversokved int2 NULL,
                svokved_svokvedosn_grnipdata_grnip text NULL,
                svokved_svokvedosn_grnipdata_valid_from date NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_okveds_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            CREATE INDEX s_individual_entrepreneur_okveds_info_ogrnip_idx ON {var_schem}.s_individual_entrepreneur_okveds_info USING btree (ogrnip);
            ALTER TABLE {var_schem}.s_individual_entrepreneur_okveds_info ADD CONSTRAINT s_individual_entrepreneur_okveds_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_pension_fund_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_pension_fund_info (
                svregpf_datareg date NULL,
                svregpf_regnompf text NULL,
                svregpf_grnipdata_grnip text NULL,
                svregpf_grnipdata_valid_from date NULL,
                svregpf_svorgpf_kodpf bpchar(6) NULL,
                svregpf_svorgpf_naimpf text NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_pension_fund_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_pension_fund_info ADD CONSTRAINT s_individual_entrepreneur_pension_fund_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_registration_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_registration_info (
                svregip_dataogrnip date NULL,
                svregip_datareg date NULL,
                svregip_naimro text NULL,
                svregip_ogrnip bpchar(15) NULL,
                svregip_regnom text NULL,
                svregip_svkfh_inn bpchar(10) NULL,
                svregip_svkfh_naimulpoln text NULL,
                svregip_svkfh_ogrn bpchar(13) NULL,
                svregip_svkfh_grnipdata_grnip text NULL,
                svregip_svkfh_grnipdata_valid_from date NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_registration_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_registration_info ADD CONSTRAINT s_individual_entrepreneur_registration_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_registrator_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_registrator_info (
                svregorg_adrro text NULL,
                svregorg_kodno int4 NULL,
                svregorg_naimno text NULL,
                svregorg_grnipdata_grnip text NULL,
                svregorg_grnipdata_valid_from date NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_registrator_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_registrator_info ADD CONSTRAINT s_individual_entrepreneur_registrator_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_social_insurance_fund_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_social_insurance_fund_info (
                svregfss_datareg date NULL,
                svregfss_regnomfss bpchar(15) NULL,
                svregfss_grnipdata_grnip text NULL,
                svregfss_grnipdata_valid_from date NULL,
                svregfss_svorgfss_kodfss bpchar(4) NULL,
                svregfss_svorgfss_naimfss text NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_social_insurance_fund_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_social_insurance_fund_info ADD CONSTRAINT s_individual_entrepreneur_social_insurance_fund_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_state_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_state_info (
                svstatus_svstatus_naimstatus text NULL,
                svstatus_svstatus_kodstatus int4 NULL,
                svstatus_grnipdata_grnip text NULL,
                svstatus_grnipdata_valid_from date NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_state_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            CREATE INDEX s_individual_entrepreneur_state_info_ogrnip_idx ON {var_schem}.s_individual_entrepreneur_state_info USING btree (ogrnip);
            ALTER TABLE {var_schem}.s_individual_entrepreneur_state_info ADD CONSTRAINT s_individual_entrepreneur_state_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
        "s_individual_entrepreneur_tax_authority_registration_info": f"""
            CREATE TABLE {var_schem}.s_individual_entrepreneur_tax_authority_registration_info (
                svucetno_datapostuc date NULL,
                svucetno_innfl bpchar(12) NULL,
                svucetno_grnipdata_grnip text NULL,
                svucetno_grnipdata_valid_from date NULL,
                svucetno_svno_kodno bpchar(4) NULL,
                svucetno_svno_naimno text NULL,
                ogrnip bpchar(15) NULL,
                individual_entrepreneur_pk bpchar(32) NOT NULL,
                load_dtm timestamp NULL,
                rec_src bpchar(5) NULL,
                hash_diff bpchar(32) NOT NULL,
                statement_dt date NULL,
                CONSTRAINT s_individual_entrepreneur_tax_authority_registration_info_pkey PRIMARY KEY (individual_entrepreneur_pk, hash_diff)
            );
            ALTER TABLE {var_schem}.s_individual_entrepreneur_tax_authority_registration_info ADD CONSTRAINT s_individual_entrepreneur_tax_authority_registration_info_fkey FOREIGN KEY (individual_entrepreneur_pk) REFERENCES {var_schem}.h_individual_entrepreneur_egrip_main(individual_entrepreneur_pk);""",
    }

    with conn_base(var_out) as conn:
        cur = conn.cursor()
        for table_name, sql_tt in tables.items():
            ssql = f"""SELECT to_regclass('{var_schem}.{table_name}') IS NOT NULL AS table_exists"""
            cur.execute(ssql)
            answer = cur.fetchone()[0]
            if answer:
                logger.info(f"Table {table_name} creation failed! Table exist!!")
            else:
                cur.execute(sql_tt)
                conn.commit()
                logger.info(f"Table {table_name} created successfully")
