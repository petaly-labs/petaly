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
    """Manages conversation history and memory for the Petaly agent."""
    
    def __init__(
        self,
        storage_path: str,
        max_entries: int = 1000,
        cleanup_threshold: float = 0.9,
        backup_enabled: bool = True
    ):
        """
        Initialize the conversation store.
        
        Args:
            storage_path: Path to store conversation history
            max_entries: Maximum number of entries to store
            cleanup_threshold: Threshold (0-1) at which to trigger cleanup
            backup_enabled: Whether to create backups before cleanup
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
        """Load conversation history from storage."""
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
        """Save conversation history to storage."""
        try:
            # Check if we need to cleanup
            if len(self.conversation_history) > self.max_entries * self.cleanup_threshold:
                self._cleanup_history()
            
            self.file_handler.save_dict_to_json(self.storage_path, self.conversation_history)
        except Exception as e:
            logger.error(f"Error saving conversation history: {e}", exc_info=True)
    
    def _create_backup(self):
        """Create a backup of the current conversation history."""
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
        """Clean up old conversation entries to maintain size limits."""
        self._create_backup()
        # Keep only the most recent entries
        self.conversation_history = self.conversation_history[-self.max_entries:]
        logger.info(f"Cleaned up conversation history, current size: {len(self.conversation_history)} entries")
    
    def add_entry(self, role: str, content: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Add a new entry to the conversation history.
        
        Args:
            role: Role of the speaker ('user' or 'assistant')
            content: Content of the message
            metadata: Optional metadata about the entry
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
        
        Args:
            count: Number of entries to retrieve
            
        Returns:
            List of recent conversation entries
        """
        return self.conversation_history[-count:]
    
    def get_entries_by_role(self, role: str) -> List[Dict[str, Any]]:
        """
        Get all entries for a specific role.
        
        Args:
            role: Role to filter by ('user' or 'assistant')
            
        Returns:
            List of entries for the specified role
        """
        return [entry for entry in self.conversation_history if entry["role"] == role]
    
    def clear_history(self):
        """Clear all conversation history."""
        self.conversation_history = []
        self._save_history()
        logger.info("Cleared conversation history")
    
    def get_history_size(self) -> int:
        """Get the current size of the conversation history."""
        return len(self.conversation_history)
    
    def get_history_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the conversation history.
        
        Returns:
            Dictionary containing history statistics
        """
        return {
            "total_entries": len(self.conversation_history),
            "user_entries": len(self.get_entries_by_role("user")),
            "assistant_entries": len(self.get_entries_by_role("assistant")),
            "oldest_entry": self.conversation_history[0]["timestamp"] if self.conversation_history else None,
            "newest_entry": self.conversation_history[-1]["timestamp"] if self.conversation_history else None
        } 