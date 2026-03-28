"""
模拟交易模块 (存根实现)
"""
import logging

logger = logging.getLogger(__name__)


class Account:
    def __init__(self, account_id: int, initial_capital: float = 1000000):
        self.account_id = account_id
        self.capital = initial_capital
        self.positions = {}
    
    def __repr__(self):
        return f"Account(id={self.account_id}, capital={self.capital})"


class AccountManager:
    def __init__(self):
        self._accounts = {1: Account(1, 1000000)}
        logger.info("AccountManager initialized with default account 1")
    
    def get_account(self, account_id: int):
        if account_id not in self._accounts:
            logger.warning(f"Account {account_id} not found, creating new account")
            self._accounts[account_id] = Account(account_id)
        return self._accounts[account_id]
    
    def list_accounts(self):
        return list(self._accounts.values())


account_manager = AccountManager()
