import bcrypt

def hash_password(password: str) -> bytes:
    password_bytes = password.encode("utf-8")
    hashed = bcrypt.hashpw(password_bytes, bcrypt.gensalt())
    return hashed

def verify_password(password: str, hashed_password: bytes) -> bool:
    password_bytes = password.encode("utf-8")
    return bcrypt.checkpw(password_bytes, hashed_password)

if __name__ == "__main__":
    # Pedir contraseña al usuario
    password = input("Ingresa tu contraseña: ")

    # Hashear
    hashed = hash_password(password)

    print("\nContraseña hasheada:")
    print(hashed)

    # Verificación opcional
    password_check = input("\nVuelve a ingresar la contraseña para verificar: ")

    if verify_password(password_check, hashed):
        print("✅ Contraseña correcta")
    else:
        print("❌ Contraseña incorrecta")
