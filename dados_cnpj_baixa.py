# -*- coding: utf-8 -*-
"""
lista relação de arquivos na página de dados públicos da receita federal
e faz o download
Os arquivos estão disponíveis em
https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj
Houve alteração de layout da página de download em fev/2026.
Talvez seja necessário ajustar o valor do parâmetro share_token na função consulta_base_webdap
"""
import requests, os, sys, time, glob, re

pasta_zip = r"dados-publicos-zip" #local dos arquivos zipados da Receita
pasta_cnpj = 'dados-publicos'

def requisitos(anoMes):
    #se pastas não existirem, cria automaticamente
    os.makedirs(pasta_cnpj, exist_ok=True)
    os.makedirs(pasta_zip, exist_ok=True)
    marcador = os.path.join(pasta_zip, '.referencia')
    anterior = open(marcador).read().strip() if os.path.exists(marcador) else None
    zips = glob.glob(os.path.join(pasta_zip, '*.zip')) + glob.glob(os.path.join(pasta_zip, '*.part'))
    if zips and anterior is not None and anterior != anoMes:
        #zips de outro mês (ou de origem desconhecida) não podem ser misturados com os novos
        print(f'Apagando {len(zips)} arquivos da pasta {pasta_zip} (referência anterior: {anterior}, atual: {anoMes})')
        for arq in zips:
            os.remove(arq)
    with open(marcador, 'w') as f:
        f.write(anoMes)
#.def requisitos

def consulta_base_webdap(share_token="gn672Ad4CF8N6TK", base_url="https://arquivos.receitafederal.gov.br/public.php/webdav/Dados/Cadastros/CNPJ"):
    # código adaptado de github.com/caiopizzol/cnpj-data-pipeline/blob/main/downloader.py
    from xml.etree import ElementTree
    DAV_NS = {"d": "DAV:"} # WebDAV XML namespace
    url = base_url + "/"
    response = requests.request("PROPFIND", url, auth=(share_token, ""), headers={"Depth": "1"}) #, timeout=(self.config.connect_timeout, self.config.read_timeout))
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)
    
    directories = []
    for response in root.findall("d:response", DAV_NS):
        href = response.find("d:href", DAV_NS).text
        match = re.search(r"(\d{4}-\d{2})/?$", href) # Match YYYY-MM directory pattern from href path
        if match:
            directories.append(match.group(1))
            
    ultimoAnoMes = directories[-1]
    #obtem lista de arquivos
    response = requests.request("PROPFIND", url+ultimoAnoMes+"/", auth=(share_token, ""), headers={"Depth": "1"}) # ,timeout=(self.config.connect_timeout, self.config.read_timeout) )
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)
    
    files = []
    tamanhos = {}
    for response in root.findall("d:response", DAV_NS):
        href = response.find("d:href", DAV_NS).text
        # Extract .zip filenames from href
        match = re.search(r"/([^/]+\.zip)$", href, re.IGNORECASE)
        if match:
            files.append(match.group(1))
            tam = response.find(".//d:getcontentlength", DAV_NS)
            tamanhos[match.group(1)] = int(tam.text) if tam is not None and tam.text else None

    #urlBaseArquivosDoMes = f'https://arquivos.receitafederal.gov.br/public.php/dav/files/{share_token}/{ultimoAnoMes}/' #para baixar usando requests sem webdav
    #urlPaginaDownloadMeses = f'https://arquivos.receitafederal.gov.br/index.php/s/{share_token}'
    #novos links a partir de 2026-10-12
    #urlBaseArquivosDoMes = f'https://arquivos.receitafederal.gov.br/index.php/s/{share_token}?dir=/Dados/Cadastros/CNPJ/{ultimoAnoMes}/' #para baixar usando requests sem webdav
    urlBaseArquivosDoMes = f'https://arquivos.receitafederal.gov.br/public.php/dav/files/{share_token}/Dados/Cadastros/CNPJ/{ultimoAnoMes}/'
    urlPaginaDownloadMeses = f'https://arquivos.receitafederal.gov.br/index.php/s/{share_token}?dir=/Dados/Cadastros/CNPJ'
    
    #return {'ultimoAnoMes':ultimoAnoMes, 'arquivos':files}
    return {'anoMes':ultimoAnoMes, 'urlBaseArquivosDoMes':urlBaseArquivosDoMes, 'arquivos':files, 'tamanhos':tamanhos, 'urlPaginaDownloadMeses':urlPaginaDownloadMeses}
#.def consulta_base_webdap

def consulta_base(bwebdap=True):
    #consulta primeiro por webdap, se não der certo abre a página com webdriver no modo headless
    r = None
    if bwebdap:
        try:
            r = consulta_base_webdap()
            print('Consulta por webdap')
        except Exception as e:
            print(f"Error: {e}")    
            print('Não conseguiu obter a data da base pelo método webdap...')
            print('Navegue até a página https://arquivos.receitafederal.gov.br/ e localize a pasta Dados>Cadastros>CNPJ')
            print('A url da página conterá um código (user ou share_token) após https://arquivos.receitafederal.gov.br/index.php/s/')
            print('Copie o user e atualize este código.') 
    return r
#.def consulta_base

print(time.asctime(), f'Início de {sys.argv[0]}:')

parametrosSite = consulta_base()
if not parametrosSite:
    r = input('Pressione Enter.')
    sys.exit(1)   
    
ultima_referencia = parametrosSite['anoMes']
urlBaseArquivosDoMes = parametrosSite['urlBaseArquivosDoMes']
print('Última base disponível:', ultima_referencia)
requisitos(ultima_referencia)

lista = [urlBaseArquivosDoMes+arq for arq in parametrosSite['arquivos']]

print('Relação de Arquivos disponíveis:')
for k in lista:
    print(k)

if __name__ == '__main__':        
    resp = input(f'Deseja baixar os arquivos acima para a pasta {pasta_zip} (y/n)?')
    if resp.lower()!='y' and resp.lower()!='s':
        sys.exit()
        
print(time.asctime(), 'Início do Download dos arquivos...')

def baixa_arquivo(url, destino, tamanho, tentativas=6):
    """Baixa para destino+'.part' e só renomeia se o tamanho bater com o da listagem."""
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36", "Accept": "*/*"}
    for tentativa in range(1, tentativas+1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=(15, 60)) as r:
                r.raise_for_status()
                total = int(r.headers.get('content-length') or tamanho or 0)
                baixado = 0
                with open(destino + '.part', 'wb') as f:
                    for pedaco in r.iter_content(1 << 20):
                        f.write(pedaco)
                        baixado += len(pedaco)
                        print(f"\r  {os.path.basename(destino)}: {baixado>>20}/{total>>20} MB", end='', flush=True)
            if tamanho and baixado != tamanho:
                raise IOError(f'tamanho {baixado} != esperado {tamanho}')
            os.replace(destino + '.part', destino)
            print(' ok')
            return True
        except Exception as e:
            espera = 5 * tentativa
            print(f'\n  falha ({type(e).__name__}: {e}); tentativa {tentativa}/{tentativas}, aguardando {espera}s')
            time.sleep(espera)
    return False

falhas = []
for k, url in enumerate(lista, 1):
    nome = os.path.split(url)[1]
    destino = os.path.join(pasta_zip, nome)
    tamanho = parametrosSite['tamanhos'].get(nome)
    if os.path.exists(destino) and (not tamanho or os.path.getsize(destino) == tamanho):
        print(f'[{k}/{len(lista)}] {nome}: já baixado, pulando')
        continue
    print(f'[{k}/{len(lista)}] {nome}')
    if not baixa_arquivo(url, destino, tamanho):
        falhas.append(nome)
if falhas:
    print('\nNÃO BAIXOU:', ', '.join(falhas), '\nRode o script novamente: os arquivos já completos são pulados.')
    sys.exit(1)

# else: #baixar sequencial, rotina antiga
#     def bar_progress(current, total, width=80):
#         if total>=2**20:
#             tbytes='Megabytes'
#             unidade = 2**20
#         else:
#             tbytes='kbytes'
#             unidade = 2**10
#         progress_message = f"Baixando: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unidade, total//unidade)
#         # Don't use print() as it will print in new line every time.
#         sys.stdout.write("\r" + progress_message)
#         sys.stdout.flush()
      
#     for k, url in enumerate(lista):
#         print('\n' + time.asctime() + f' - item {k}: ' + url)
#         wget.download(url, out=os.path.join(pasta_zip, os.path.split(url)[1]), bar=bar_progress)

        
print('\n\n'+ time.asctime(), f' Finalizou {sys.argv[0]}!!!')
print(f"Baixou {len(glob.glob(os.path.join(pasta_zip,'*.zip')))} arquivos.")
if __name__ == '__main__':
    input('Pressione Enter')

