from typing import Dict, List, Optional
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from dataclasses import dataclass


@dataclass
class KafkaSettings:
    brokers: str = "77.81.230.104:9092"
    user: str = "admin"
    password: str = "VawEzo1ikLtrA8Ug8THa"
    protocol: str = "SASL_PLAINTEXT"
    mechanism: str = "PLAIN"
    prefix: str = "kachurovskyi"

    def admin_config(self) -> Dict[str, str]:
        return {
            "bootstrap_servers": self.brokers,
            "security_protocol": self.protocol,
            "sasl_mechanism": self.mechanism,
            "sasl_plain_username": self.user,
            "sasl_plain_password": self.password,
        }

class KafkaManager:
    def __init__(self, settings: KafkaSettings):
        self.settings = settings
        self.client = KafkaAdminClient(**settings.admin_config())

    def shutdown(self):
        if self.client:
            self.client.close()

    def remove_topics(self, topics: List[str]):
        try:
            available_topics = self.client.list_topics()
            topics_to_remove = [t for t in topics if t in available_topics]

            if topics_to_remove:
                self.client.delete_topics(topics_to_remove)
                for topic in topics_to_remove:
                    print(f"Successfully removed topic: {topic}")
            else:
                print("No topics to remove.")
        except KafkaError as error:
            print(f"Error while removing topics: {error}")
        except Exception as ex:
            print(f"Unexpected error during topic removal: {ex}")

    def add_topics(self, topics: Dict[str, str]):
        try:
            new_topics = [
                NewTopic(name=topic, num_partitions=1, replication_factor=1)
                for topic in topics
            ]

            self.client.create_topics(new_topics)
            for topic in topics:
                print(f"Successfully added topic: {topic}")
        except KafkaError as error:
            print(f"Error while adding topics: {error}")
        except Exception as ex:
            print(f"Unexpected error during topic creation: {ex}")

    def fetch_topics(self) -> Optional[List[str]]:
        try:
            topics = self.client.list_topics()
            matching = [t for t in topics if self.settings.prefix in t]

            for topic in matching:
                print(f"Topic found: {topic}")
            return matching
        except KafkaError as error:
            print(f"Error while fetching topics: {error}")
        except Exception as ex:
            print(f"Unexpected error during topic fetching: {ex}")
        return None

def run():
    manager = None
    try:
        settings = KafkaSettings()

        topic_definitions = {
            f"{settings.prefix}_results": "Results topic",
            f"{settings.prefix}_metrics": "Metrics topic",
        }

        manager = KafkaManager(settings)
        print("Managing Kafka topics...")

        manager.remove_topics(list(topic_definitions.keys()))
        manager.add_topics(topic_definitions)
        manager.fetch_topics()

    except Exception as ex:
        print(f"Error during Kafka operations: {ex}")
        raise
    finally:
        if manager:
            manager.shutdown()

if __name__ == "__main__":
    run()