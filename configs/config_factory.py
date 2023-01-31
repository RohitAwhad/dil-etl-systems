from constants.entity_enum import EntityTypeEnum
from configs.system_config import get_system_config
from configs.hospital_config import get_hospital_config
from configs.state_config import get_state_config

def get_config(entity_type_enum):
    if entity_type_enum == EntityTypeEnum.SYSTEM:
        return get_system_config()
    elif entity_type_enum == EntityTypeEnum.HOSPITAL:
        return get_hospital_config()
    elif entity_type_enum == EntityTypeEnum.STATE:
        return get_state_config()