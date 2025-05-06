from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
df_meses_2017_mandioqueiro = DATA_DIR / 'df_meses_2017_mandioqueiro'

df_time_s_entrada = DATA_DIR / 'df_time_s_entrada.csv'
df_time_s_saida = DATA_DIR / 'df_time_s_saida.csv'
segundo_df_time_s_entrada = DATA_DIR / 'segundo_df_time_s_entrada.csv'
segundo_df_time_s_saida = DATA_DIR / 'segundo_df_time_s_saida.csv'

historial_cambios_me_epp_solo = DATA_DIR / "historial_cambios_me_epp_solo.csv"

if __name__ == "__main__":
    print(df_meses_2017_mandioqueiro, DATA_DIR)