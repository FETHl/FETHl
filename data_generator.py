import time
import os
import re
from kafka import KafkaProducer
from kafka.errors import KafkaError
import traceback

def create_kafka_producer(bootstrap_servers="kafka:9092", retries=3, retry_interval=5):
    """Crée et retourne un producteur Kafka avec tentatives de reconnexion"""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            print("Kafka producer created successfully")
            return producer
        except KafkaError as ke:
            print(f"Tentative {attempt+1}/{retries}: Erreur lors de la création du producteur Kafka: {str(ke)}")
            if attempt < retries - 1:
                print(f"Nouvelle tentative dans {retry_interval} secondes...")
                time.sleep(retry_interval)
            else:
                print("Échec de la création du producteur Kafka après plusieurs tentatives")
                print(traceback.format_exc())
                return None

def is_apache_log(line):
    """Vérifie si une ligne est au format de log Apache/Nginx"""
    # Format commun pour les logs Apache/Nginx
    apache_pattern = r'^\S+ - - \[\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4}\] "\w+ .* HTTP/\d\.\d" \d+ \d+.*$'
    return bool(re.match(apache_pattern, line))

def read_logs(file_path, producer, topic="logs", position_file=".log_position"):
    """Lit les logs depuis un fichier et les envoie à Kafka en gardant la position"""
    try:
        # Vérifier si le fichier existe
        if not os.path.exists(file_path):
            print(f"Erreur: Le fichier {file_path} n'existe pas. En attente de sa création...")
            while not os.path.exists(file_path):
                time.sleep(1)
            print(f"Le fichier {file_path} a été créé. Démarrage du traitement...")

        # Récupérer la dernière position connue
        last_position = 0
        last_inode = None
        if os.path.exists(position_file):
            try:
                with open(position_file, 'r') as pos_file:
                    content = pos_file.readline().strip()
                    if content:
                        parts = content.split(':')
                        if len(parts) == 2:
                            last_inode = int(parts[0])
                            last_position = int(parts[1])
            except Exception as e:
                print(f"Erreur lors de la lecture du fichier de position: {str(e)}")

        # Obtenir l'inode actuel du fichier de logs
        current_inode = os.stat(file_path).st_ino
        
        # Si le fichier a été modifié (rotation), commencer depuis le début
        if last_inode is not None and last_inode != current_inode:
            print(f"Détection de rotation de fichier (ancien inode: {last_inode}, nouveau: {current_inode})")
            last_position = 0
        
        print(f"Lecture des logs depuis le fichier {file_path} à partir de la position {last_position}")
        
        with open(file_path, 'r') as file:
            # Se positionner au dernier endroit connu
            if last_position > 0:
                file.seek(last_position)
            
            while True:
                current_position = file.tell()
                line = file.readline()
                
                if line:
                    # Nettoyer la ligne (supprimer les caractères de nouvelle ligne)
                    line = line.strip()
                    
                    if line:  # Ne pas envoyer de lignes vides
                        try:
                            # Envoyer la ligne brute à Kafka
                            producer.send(topic, line.encode("utf-8"))
                            
                            # Flush périodiquement pour garantir que les messages sont envoyés
                            if current_position % 10 == 0:
                                producer.flush()
                                
                            # Déterminer le type de log pour un meilleur affichage
                            if is_apache_log(line):
                                # Extraction du code d'état HTTP pour coloration
                                status_match = re.search(r'" (\d+) ', line)
                                status_code = int(status_match.group(1)) if status_match else 0
                                
                                # Affichage avec couleur selon le code d'état
                                if status_code >= 500:
                                    status_indicator = "🔴"  # Rouge pour erreurs serveur
                                elif status_code >= 400:
                                    status_indicator = "🟠"  # Orange pour erreurs client
                                elif status_code >= 300:
                                    status_indicator = "🟡"  # Jaune pour redirections
                                elif status_code >= 200:
                                    status_indicator = "🟢"  # Vert pour succès
                                else:
                                    status_indicator = "⚪"  # Blanc pour autres
                                    
                                print(f"{status_indicator} {line[:100]}..." if len(line) > 100 else f"{status_indicator} {line}")
                            else:
                                # Pour les lignes qui ne sont pas des logs Apache/Nginx
                                print(f"📋 {line[:100]}..." if len(line) > 100 else f"📋 {line}")
                                
                        except KafkaError as ke:
                            print(f"Erreur lors de l'envoi du message à Kafka: {str(ke)}")
                            print(traceback.format_exc())
                    
                    # Sauvegarder la position actuelle
                    with open(position_file, 'w') as pos_file:
                        pos_file.write(f"{current_inode}:{file.tell()}")
                    
                else:
                    # Vérifier si le fichier a été tronqué (rotation de logs sans changement d'inode)
                    file_size = os.path.getsize(file_path)
                    if file_size < current_position:
                        print("Fichier tronqué détecté. Réouverture du fichier...")
                        file.close()
                        with open(position_file, 'w') as pos_file:
                            pos_file.write(f"{current_inode}:0")
                        break  # Sortir de la boucle pour réouvrir le fichier
                    
                    # Si pas de nouvelle ligne, attend un peu avant de réessayer
                    time.sleep(0.1)
                    
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier: {str(e)}")
        print(traceback.format_exc())

if __name__ == "__main__":
    LOG_FILE = "web_server.log"  # Nom du fichier de log à surveiller
    KAFKA_TOPIC = "logs"         # Topic Kafka pour les logs (doit correspondre à celui dans le script Spark)
    BOOTSTRAP_SERVERS = "kafka:9092"  # Serveur(s) Kafka
    
    print(f"Démarrage du producteur Kafka - Lecture depuis {LOG_FILE} vers le topic {KAFKA_TOPIC}")
    
    try:
        producer = create_kafka_producer(bootstrap_servers=BOOTSTRAP_SERVERS)
        if producer:
            while True:  # Boucle principale pour redémarrer la lecture en cas d'erreur
                try:
                    read_logs(LOG_FILE, producer, topic=KAFKA_TOPIC)
                except Exception as e:
                    print(f"Erreur pendant la lecture des logs: {str(e)}")
                    print(traceback.format_exc())
                    print("Reprise de la lecture dans 5 secondes...")
                    time.sleep(5)
        else:
            print("Le producteur Kafka n'a pas pu être créé. Fermeture du programme.")
    except KeyboardInterrupt:
        print("Programme interrompu par l'utilisateur. Nettoyage...")
        if 'producer' in locals() and producer:
            producer.flush()
            producer.close()
        print("Programme terminé.")
    except Exception as e:
        print(f"Erreur dans le programme principal: {str(e)}")
        print(traceback.format_exc())