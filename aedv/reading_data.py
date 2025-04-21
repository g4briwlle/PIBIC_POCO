from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
df_meses_2017_mandioqueiro = DATA_DIR / 'df_meses_2017_mandioqueiro'
historial_cambios_me_epp_solo = DATA_DIR / "historial_cambios_me_epp_solo.csv"

if __name__ == "__main__":
    print(df_meses_2017_mandioqueiro, DATA_DIR)