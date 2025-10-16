# Дополнительные правки для utils/config.py

import asyncio
import os
import json
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv
from loguru import logger

class Config:
    """Singleton для конфигурации"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        load_dotenv()

        # Загружаем переменные окружения
        self.bot_token: str = os.getenv("BOT_TOKEN", "")
        self.config_path: str = os.getenv("BOT_CONFIG_PATH", "bot_config.json")
        self._config_data: Dict[str, Any] = {}
        
        # Загружаем ID администраторов из списка, разделенного запятыми
        admin_ids_str = os.getenv("ADMIN_IDS", os.getenv("OWNER_ID", ""))
        self.admin_ids: List[int] = []
        
        if admin_ids_str:
            try:
                # Разделяем по запятым и конвертируем в целые числа
                self.admin_ids = [int(id.strip()) for id in admin_ids_str.split(',') if id.strip()]
            except ValueError as e:
                logger.error(f"Ошибка при парсинге ID администраторов: {e}")
                # В случае ошибки используем пустой список
                self.admin_ids = []
        
        # Для обратной совместимости - сохраняем первого администратора как owner_id
        self.owner_id: int = self.admin_ids[0] if self.admin_ids else 0

        self.source_channels: List[str] = []

        # Поддержка обратной совместимости - добавляем начальный канал источника, если он указан
        initial_source = os.getenv("SOURCE_CHANNEL", "").lstrip('@')
        if initial_source:
            self.source_channels.append(initial_source)
            
        self.db_path: str = os.getenv("DB_PATH", "forwarder.db")
        
        # Пытаемся загрузить дополнительные каналы из конфигурационного файла
        self._load_config()
        
        # Проверяем обязательные настройки
        if not all([self.bot_token, self.admin_ids]):
            raise ValueError("Отсутствуют обязательные переменные окружения")
        
        # Настройки кэша
        self.cache_ttl: int = 300  # 5 минут кэширования для информации о чатах
        self.max_cache_size: int = 100
        
        # Настройки соединений с базой данных
        self.max_db_connections: int = 5
        
        # Гарантируем наличие файла конфигурации с актуальными данными
        self._save_config()

        self._initialized = True
    
    def is_admin(self, user_id: int) -> bool:
        """Проверка, является ли пользователь администратором"""
        return user_id in self.admin_ids
    
    def _load_config(self) -> None:
        """Загрузка конфигурации из файла"""
        default_config: Dict[str, Any] = {
            "source_channels": [],
            "target_chats": [],
            "last_message_ids": {},
            "admin_ids": []
        }

        data: Dict[str, Any] = {}

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    data = {}
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}

        # Объединяем с конфигурацией по умолчанию
        for key, default_value in default_config.items():
            value = data.get(key, default_value)
            if isinstance(default_value, list):
                value = value if isinstance(value, list) else list(default_value)
            elif isinstance(default_value, dict):
                value = value if isinstance(value, dict) else dict(default_value)
            data[key] = value

        self._config_data = data

        # Загружаем каналы из файла (добавляем к уже загруженным из окружения)
        for channel in data.get('source_channels', []):
            channel = str(channel).lstrip('@')
            if channel and channel not in self.source_channels:
                self.source_channels.append(channel)

        # Загружаем дополнительные администраторы из файла
        file_admins = []
        for admin_id in data.get('admin_ids', []):
            try:
                admin_int = int(str(admin_id).strip())
                file_admins.append(admin_int)
            except (TypeError, ValueError):
                logger.warning(f"Некорректный ID администратора в конфиге: {admin_id}")

        if file_admins:
            combined = set(self.admin_ids) | set(file_admins)
            self.admin_ids = sorted(combined)
            if self.admin_ids:
                self.owner_id = self.admin_ids[0]

    def _save_config(self) -> None:
        """Сохранение текущей конфигурации"""
        try:
            config_copy = dict(self._config_data)
            config_copy['source_channels'] = self.source_channels
            config_copy.setdefault('target_chats', [])
            config_copy.setdefault('last_message_ids', {})
            config_copy['admin_ids'] = [int(admin_id) for admin_id in self.admin_ids]

            os.makedirs(os.path.dirname(self.config_path) or '.', exist_ok=True)

            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config_copy, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Не удалось сохранить конфигурацию: {e}")
            
    def add_source_channel(self, channel: str) -> bool:
        """Добавление нового канала источника и сохранение в конфиг"""
        channel = channel.lstrip('@')
        if channel and channel not in self.source_channels:
            self.source_channels.append(channel)
            self._save_config()
            return True
        return False
        
    def remove_source_channel(self, channel: str) -> bool:
        """Удаление канала источника и обновление конфига"""
        channel = channel.lstrip('@')
        if channel in self.source_channels:
            self.source_channels.remove(channel)
            self._save_config()
            return True
        return False

    def add_admin(self, user_id: int) -> bool:
        """Добавление нового администратора с сохранением"""
        if user_id in self.admin_ids:
            return False

        self.admin_ids.append(user_id)
        self.admin_ids = sorted(set(self.admin_ids))
        self.owner_id = self.admin_ids[0] if self.admin_ids else 0
        self._save_config()
        return True