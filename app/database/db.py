from sqlalchemy import create_engine, text
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def get_db_connection():
    try:
        print("🔄 Tentative de connexion à la base de données PostgreSQL...")
        
        # Créer l'engine avec SQLAlchemy
        engine = create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )

        # Vérifier la connexion
        with engine.connect() as connection:
            print(f"✅ Connexion établie avec succès à la base '{DB_NAME}' sur le serveur '{DB_HOST}:{DB_PORT}'")

            # Vérifier les tables disponibles
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")).fetchall()
            table_list = [table[0] for table in result]
            print(f"🗃️ Tables disponibles : {', '.join(table_list)}")

        print("🚀 La connexion est prête à être utilisée.")
        return engine

    except Exception as e:
        print(f"❌ Erreur lors de la connexion à la base de données : {e}")
        return None