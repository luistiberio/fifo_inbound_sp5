import asyncio
import os
import shutil
import time
import datetime
import gc
import traceback
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import zipfile
from gspread_dataframe import set_with_dataframe
from playwright.async_api import async_playwright

# =================== CONFIGURAÇÕES ===================
DOWNLOAD_DIR = "/tmp/shopee_automation"
SPREADSHEET_ID = "1Ie3u58e-PT1ZEQJE20a6GJB-icJEXBRVDVxTzxCqq4c"
ABA_NOME = "Base"
OPS_ID = os.environ.get('OPS_ID')
OPS_SENHA = os.environ.get('OPS_SENHA')
# =====================================================

def rename_downloaded_file(DOWNLOAD_DIR, download_path):
    """Renames the downloaded file to include the current hour."""
    try:
        current_hour = datetime.datetime.now().strftime("%H")
        new_file_name = f"TO-Packed{current_hour}.zip"
        
        # CORREÇÃO: Usar DOWNLOAD_DIR (maiúsculo) conforme o argumento da função
        new_file_path = os.path.join(DOWNLOAD_DIR, new_file_name)
        
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
            
        shutil.move(download_path, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
        return new_file_path
    except Exception as e:
        # Aqui o erro aconteceu porque download_dir não existia
        print(f"Erro ao renomear o arquivo: {e}")
        return None

def unzip_and_process_data(zip_path, extract_to_dir):
    """
    Descompacta um ZIP, junta todos os CSVs e aplica o filtro de colunas.
    """
    try:
        unzip_folder = os.path.join(extract_to_dir, "extracted_files")
        os.makedirs(unzip_folder, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_folder)
        print(f"📂 Arquivo '{os.path.basename(zip_path)}' descompactado.")

        csv_files = [os.path.join(unzip_folder, f) for f in os.listdir(unzip_folder) if f.lower().endswith('.csv')]
        
        if not csv_files:
            print(f"⚠ Nenhum CSV encontrado no {zip_path}")
            shutil.rmtree(unzip_folder)
            return None

        print(f"📑 Lendo e unificando {len(csv_files)} arquivos CSV...")
        all_dfs = [pd.read_csv(file, encoding='utf-8') for file in csv_files]
        df_final = pd.concat(all_dfs, ignore_index=True)

        print("🔎 Aplicando filtros de colunas...")
        indices_para_manter = [0, 15, 39, 40, 48] # [0, 11, 36, 37, 45]
        df_final = df_final.iloc[:, indices_para_manter]

        shutil.rmtree(unzip_folder)  # limpa apenas a pasta de extração
        return df_final
    except Exception as e:
        print(f"❌ Erro processando {zip_path}: {e}")
        return None

def update_google_sheet_with_dataframe(df_to_upload):
    """Atualiza Google Sheets em uma aba fixa."""
    if df_to_upload is None or df_to_upload.empty:
        print(f"⚠ Nenhum dado para enviar para a aba '{ABA_NOME}'.")
        return
        
    try:
        print(f"⬆ Enviando dados para a aba '{ABA_NOME}'...")

        df_to_upload = df_to_upload.fillna("").astype(str)

        scope = [
            "https://spreadsheets.google.com/feeds",
            'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive"
        ]
        if not os.path.exists("hxh.json"):
            raise FileNotFoundError("O arquivo 'hxh.json' não foi encontrado.")

        creds = Credentials.from_service_account_file("hxh.json", scopes=scope)
        client = gspread.authorize(creds)
        
        planilha = client.open_by_key(SPREADSHEET_ID)

        # Cria ou pega a aba pelo nome definido
        try:
            aba = planilha.worksheet(ABA_NOME)
        except gspread.exceptions.WorksheetNotFound:
            aba = planilha.add_worksheet(title=ABA_NOME, rows="1000", cols="20")
        
        aba.clear()
        set_with_dataframe(aba, df_to_upload)
        
        print(f"✅ Dados enviados com sucesso para '{ABA_NOME}'!")
    except Exception as e:
        import traceback
        print(f"❌ Erro ao enviar para Google Sheets na aba '{ABA_NOME}':\n{traceback.format_exc()}")

async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


    async with async_playwright() as p:
        # Mantive os parâmetros de segurança e pop-up que funcionaram no código anterior
        browser = await p.chromium.launch(
            headless=False, 
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"]
        )
        context = await browser.new_context(accept_downloads=True, viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        try:
            d1 = 'SoC_SP_Cravinhos'
            # === LOGIN ===
            print("Realizando login...")
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill(OPS_ID)
            await page.locator('xpath=//*[@placeholder="Senha"]').fill(OPS_SENHA)
            await page.wait_for_timeout(5000)
            await page.get_by_role('button', name='Entrar').click(force=True)
            #await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_timeout(10000)
            
            # Tentar fechar popup se existir
            try:
                if await page.locator('.ssc-dialog-close').is_visible():
                    await page.locator('.ssc-dialog-close').click()
            except:
                pass
            
            # === NAVEGAÇÃO E EXPORTAÇÃO ===
            print("Navegando...")
            await page.goto("https://spx.shopee.com.br/#/orderTracking")
            await page.wait_for_timeout(8000)
            
            # Tratamento de Pop-up extra antes de exportar
            try:
                if await page.locator('.ssc-dialog-wrapper').is_visible():
                     await page.keyboard.press("Escape")
                     await page.wait_for_timeout(1000)
            except:
                pass

            '''
            print("Exportando...")
            await page.wait_for_timeout(5000)

            # await page.locator("span").filter(has_text="Exportar").first.click(force=True)
            await page.locator('xpath=/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[8]/div[1]/span[1]/div[2]/span[1]/span[1]/span[2]/button[1]').click()
            # await page.locator("span").filter(has_text="Exportar").nth(2).click()
            # await page.get_by_role('button', name='Exportar').click(force=True) #ok

            '''

            print("Exportando... (TESTE)")

            frames = page.frames
            print(f"Total de frames detectados: {len(frames)}")
            for i, frame in enumerate(frames):
                print(f"Frame {i}: {frame.name} - {frame.url}")

            count = await page.locator('span', has_text='Exportar').count()
            print(f'Total de spans com "Exportar": {count}')
            count1 = await page.get_by_role('button', name='Exportar').count()
            print(f'Total de botão com "Exportar": {count1}')

            # await page.locator("button.batch-actions-btn").click()
            await page.get_by_role('button', name='Exportar').click(force=True) #ok
            await page.wait_for_timeout(5000)

            
            # Clicar no elemento com texto "Exportar Pedido Avançado"
            await page.get_by_text("Exportar Pedido Avançado").click()
            #await page.locator('xpath=/html[1]/body[1]/span[4]/div[1]/div[1]/div[1]').click(force=True)
            await page.wait_for_timeout(5000)
            await page.get_by_role("treeitem", name="SOC_Received", exact=True).click(force=True)
            await page.wait_for_timeout(5000)
            
            # Seleção Cravinhos

            await page.get_by_text("+ adicionar à").nth(2).click()
            await page.wait_for_timeout(5000)


            '''
            input1 = page.locator('xpath=/html[1]/body[1]/span[8]/div[1]/div[1]/div[1]/input[1]') # "procurar por"
            input1.click()
            input1.fill(d1)
            time.sleep(5)
            '''

            await page.locator('xpath=/html/body/span[6]/div/div[1]/div/input').fill('SoC_SP_Cravinhos')

            await page.wait_for_timeout(5000)

            await page.locator('xpath=/html[1]/body[1]/span[6]/div[1]/div[2]/div[1]/ul[1]/div[1]/div[1]/li[1]').click()
            # await page.get_by_role("listitem", name="SoC_SP_Cravinhos", exact=True).click(force=True)

            await page.get_by_role("button", name="Confirmar").click(force=True)
            
            print("Aguardando geração do relatório...")
            await page.wait_for_timeout(900000) 

            # === DOWNLOAD ===
            print("Baixando...")
            async with page.expect_download(timeout=120000) as download_info:
                await page.get_by_role("button", name="Baixar").first.click(force=True)
            
            download = await download_info.value
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path)
            print(f"Download concluído: {download_path}")

            # === PROCESSAMENTO ===
            renamed_zip_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)
            
            if renamed_zip_path:
                final_dataframe = unzip_and_process_data(renamed_zip_path, DOWNLOAD_DIR)
                update_google_sheet_with_dataframe(final_dataframe)
                
                if final_dataframe is not None:
                    del final_dataframe
                    gc.collect()

        except Exception as e:
            print(f"Erro durante a execução do Playwright: {e}")
            traceback.print_exc()
        finally:
            await browser.close()
            if os.path.exists(DOWNLOAD_DIR):
                shutil.rmtree(DOWNLOAD_DIR)
                print("Limpeza concluída.")

    

'''    
    try:
        zip_files = [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR) if f.lower().endswith(".zip")]
        
        if not zip_files:
            print("⚠ Nenhum arquivo .zip encontrado na pasta.")
            return

        print(f"🔍 Encontrados {len(zip_files)} arquivos ZIP.")

        dfs = []
        for zip_path in zip_files:
            df = unzip_and_process_data(zip_path, DOWNLOAD_DIR)
            if df is not None and not df.empty:
                dfs.append(df)

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            update_google_sheet_with_dataframe(df_final)
        else:
            print("⚠ Nenhum dado válido processado.")

    except Exception as e:
        print(f"❌ Erro no processo principal: {e}")
'''

if __name__ == "__main__":
    asyncio.run(main())
