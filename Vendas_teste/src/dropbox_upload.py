import dropbox
from dropbox.exceptions import ApiError

def upload_para_dropbox(caminho_local, caminho_remoto, token):
    dbx = dropbox.Dropbox(token)
    
    try:
        with open(caminho_local, "rb") as f:
            dbx.files_upload(f.read(), caminho_remoto, mode=dropbox.files.WriteMode.overwrite)
    except FileNotFoundError:
        print(f"❌ Arquivo {caminho_local} não encontrado.")
        return None
    except Exception as e:
        print(f"❌ Erro ao fazer upload: {e}")
        return None

    try:
        # Tenta criar link compartilhável
        shared_link = dbx.sharing_create_shared_link_with_settings(caminho_remoto)
        return shared_link.url
    except ApiError as e:
        # Se link já existir, retorna o link existente
        if e.error.is_shared_link_already_exists():
            links = dbx.sharing_list_shared_links(path=caminho_remoto).links
            if links:
                return links[0].url
        print(f"❌ Erro ao criar link compartilhável: {e}")
        return None
