if st.button("Entrar"):
    usuario_lower = usuario.lower()
    print(f"🔐 Tentativa login: {usuario_lower}")
    print(f"🔐 Usuários disponíveis: {list(usuarios.keys())}")
    
    if usuario_lower in usuarios:
        print(f"✅ Usuário encontrado: {usuario_lower}")
        print(f"🔑 Senha esperada: {usuarios[usuario_lower]['password']}")
        print(f"🔑 Senha fornecida: {senha}")