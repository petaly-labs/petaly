# Copyright © 2024-2025 Pavel Rabaev
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from petaly.utils.file_handler import FileHandler

logger = logging.getLogger(__name__)

class ConversationStore:
    """
    Manages conversation history and memory for the Petaly agent.
    Handles storage, retrieval, and cleanup of conversation entries.
    Supports backup creation and size management.
    """
    
    def __init__(
        self,
        storage_path: str,
        max_entries: int = 1000,
        cleanup_threshold: float = 0.9,
        backup_enabled: bool = True
    ):
        """
        Initialize the conversation store.
        
        Logic:
        1. Set storage parameters
        2. Create storage directory
        3. Load existing history
        """
        self.storage_path = storage_path
        self.max_entries = max_entries
        self.cleanup_threshold = cleanup_threshold
        self.backup_enabled = backup_enabled
        self.file_handler = FileHandler()
        
        # Ensure storage directory exists
        os.makedirs(os.path.dirname(storage_path), exist_ok=True)
        
        # Load existing conversation history
        self.conversation_history = self._load_history()
    
    def _load_history(self) -> List[Dict[str, Any]]:
        """
        Load conversation history from storage.
        
        Logic:
        1. Check if history file exists
        2. Load and validate history
        3. Trim to max size if needed
        """
        try:
            if self.file_handler.is_file(self.storage_path):
                history = self.file_handler.load_json(self.storage_path)
                # Ensure history doesn't exceed maximum size
                if len(history) > self.max_entries:
                    history = history[-self.max_entries:]
                return history
            return []
        except Exception as e:
            logger.error(f"Error loading conversation history: {e}", exc_info=True)
            return []
    
    def _save_history(self):
        """
        Save conversation history to storage.
        
        Logic:
        1. Check cleanup threshold
        2. Perform cleanup if needed
        3. Save history to file
        """
        try:
            # Check if we need to cleanup
            if len(self.conversation_history) > self.max_entries * self.cleanup_threshold:
                self._cleanup_history()
            
            self.file_handler.save_dict_to_json(self.storage_path, self.conversation_history)
        except Exception as e:
            logger.error(f"Error saving conversation history: {e}", exc_info=True)
    
    def _create_backup(self):
        """
        Create a backup of the current conversation history.
        
        Logic:
        1. Check if backup is enabled
        2. Generate backup filename with timestamp
        3. Save current history to backup
        """
        if not self.backup_enabled:
            return
            
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{self.storage_path}.{timestamp}.bak"
            self.file_handler.save_dict_to_json(backup_path, self.conversation_history)
            logger.info(f"Created backup at {backup_path}")
        except Exception as e:
            logger.error(f"Error creating backup: {e}", exc_info=True)
    
    def _cleanup_history(self):
        """
        Clean up old conversation entries to maintain size limits.
        
        Logic:
        1. Create backup if enabled
        2. Trim history to max entries
        3. Log cleanup results
        """
        self._create_backup()
        # Keep only the most recent entries
        self.conversation_history = self.conversation_history[-self.max_entries:]
        logger.info(f"Cleaned up conversation history, current size: {len(self.conversation_history)} entries")
    
    def add_entry(self, role: str, content: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Add a new entry to the conversation history.
        
        Logic:
        1. Validate role
        2. Create entry with timestamp
        3. Add metadata if provided
        4. Save to history
        """
        if role not in {"user", "assistant"}:
            raise ValueError("Role must be either 'user' or 'assistant'")
            
        entry = {
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat()
        }
        
        if metadata:
            entry["metadata"] = metadata
            
        self.conversation_history.append(entry)
        self._save_history()
    
    def get_recent_entries(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        Get the most recent conversation entries.
        
        Logic:
        1. Get last N entries from history
        2. Return entries list
        """
        return self.conversation_history[-count:]
    
    def get_entries_by_role(self, role: str) -> List[Dict[str, Any]]:
        """
        Get all entries for a specific role.
        
        Logic:
        1. Filter history by role
        2. Return matching entries
        """
        return [entry for entry in self.conversation_history if entry["role"] == role]
    
    def clear_history(self):
        """
        Clear all conversation history.
        
        Logic:
        1. Reset history list
        2. Save empty history
        3. Log operation
        """
        self.conversation_history = []
        self._save_history()
        logger.info("Cleared conversation history")
    
    def get_history_size(self) -> int:
        """
        Get the current size of the conversation history.
        
        Logic:
        1. Return length of history list
        """
        return len(self.conversation_history)
    
    def get_history_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the conversation history.
        
        Logic:
        1. Count total entries
        2. Count entries by role
        3. Get oldest and newest timestamps
        4. Return summary dictionary
        """
        return {
            "total_entries": len(self.conversation_history),
            "user_entries": len(self.get_entries_by_role("user")),
            "assistant_entries": len(self.get_entries_by_role("assistant")),
            "oldest_entry": self.conversation_history[0]["timestamp"] if self.conversation_history else None,
            "newest_entry": self.conversation_history[-1]["timestamp"] if self.conversation_history else None
        } 