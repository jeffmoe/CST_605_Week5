import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaSetup:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.bootstrap_servers = bootstrap_servers
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='kafka-setup'
            )
        except Exception as e:
            logger.error(f"Failed to create Kafka admin client: {e}")
            self.admin_client = None
    
    def create_topic(self, topic_name, num_partitions=3, replication_factor=1):
        if not self.admin_client:
            logger.error("Kafka admin client not available")
            return False
            
        try:
            topic_list = [
                NewTopic(
                    name=topic_name,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor
                )
            ]
            
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Topic '{topic_name}' created successfully")
            return True
            
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{topic_name}' already exists")
            return True
        except Exception as e:
            logger.error(f"Error creating topic: {e}")
            return False