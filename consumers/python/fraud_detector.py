"""
Fraud Detection Module Contains all fraud detection rules and logic
"""
from datetime import datetime
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class FraudDetector:

    def __init__(self, config: Dict = None):

        self.config = config or {}
        
        self.HIGH_AMOUNT_THRESHOLD = self.config.get('high_amount', 5000)
        self.LOW_RATE_THRESHOLD = self.config.get('low_rate', 0.001)
        self.SUSPICIOUS_HOUR_START = self.config.get('suspicious_hour_start', 1)
        self.SUSPICIOUS_HOUR_END = self.config.get('suspicious_hour_end', 5)
        self.RAPID_TRANSACTION_WINDOW = self.config.get('rapid_window_seconds', 300) 
        
        # Track recent transactions per user for velocity checks
        self.user_transaction_history = {}
        
    
    def detect_fraud(self, transaction: Dict) -> tuple[bool, List[str], int]:
        """
        Run all fraud detection rules on a transaction
        
        Args:
            transaction: Enriched transaction dictionary
        
        Returns:
            Tuple of (is_fraud, list_of_flags, risk_score)
        """
        flags = []
        risk_score = 0
        
        # Rule 1: Location Mismatch 
        location_flag, location_score = self._check_location_mismatch(transaction)
        if location_flag:
            flags.append(location_flag)
            risk_score += location_score
        
        # Rule 2: High Amount
        amount_flag, amount_score = self._check_high_amount(transaction)
        if amount_flag:
            flags.append(amount_flag)
            risk_score += amount_score
        
        # Rule 3: Currency Rate Manipulation
        rate_flag, rate_score = self._check_currency_rate(transaction)
        if rate_flag:
            flags.append(rate_flag)
            risk_score += rate_score
        
        # Rule 4: Suspicious Time
        time_flag, time_score = self._check_transaction_time(transaction)
        if time_flag:
            flags.append(time_flag)
            risk_score += time_score
        
        # Rule 5: Rapid Succession (Velocity Check)
        velocity_flag, velocity_score = self._check_velocity(transaction)
        if velocity_flag:
            flags.append(velocity_flag)
            risk_score += velocity_score
        
        # Rule 6: Round Amount (Potential Test Transaction)
        round_flag, round_score = self._check_round_amount(transaction)
        if round_flag:
            flags.append(round_flag)
            risk_score += round_score
        
        # Rule 7: Missing User Profile (New/Suspicious User)
        profile_flag, profile_score = self._check_missing_profile(transaction)
        if profile_flag:
            flags.append(profile_flag)
            risk_score += profile_score
        
        is_fraud = len(flags) > 0
        
        if is_fraud:
            logger.warning(f"Fraud detected for txn {transaction['transaction_id']}: "
                          f"{flags} (Risk Score: {risk_score})")
        
        return is_fraud, flags, risk_score
    
    def _check_location_mismatch(self, txn: Dict) -> tuple[str, int]:
        """Check if transaction location doesn't match user's home country"""
        user = txn.get("user_profile", {})
        home_country = user.get("home_country")
        txn_country = txn["location"]["country"]
        
        if home_country and txn_country != home_country:
            return "LOCATION_MISMATCH", 30
        return None, 0
    
    def _check_high_amount(self, txn: Dict) -> tuple[str, int]:
        """Check for unusually high transaction amounts"""
        amount = txn["amount"]
        
        if amount > self.HIGH_AMOUNT_THRESHOLD:
            # Progressive risk based on amount
            if amount > 10000:
                return "EXTREMELY_HIGH_AMOUNT", 40
            elif amount > 7500:
                return "VERY_HIGH_AMOUNT", 30
            else:
                return "HIGH_AMOUNT", 20
        return None, 0
    
    def _check_currency_rate(self, txn: Dict) -> tuple[str, int]:
        """Check for suspiciously low exchange rates (potential manipulation)"""
        rate = txn.get("rate_to_usd")
        
        if rate is None:
            return "MISSING_EXCHANGE_RATE", 15
        
        if isinstance(rate, (int, float)) and rate < self.LOW_RATE_THRESHOLD:
            return "SUSPICIOUS_EXCHANGE_RATE", 25
        
        return None, 0
    
    def _check_transaction_time(self, txn: Dict) -> tuple[str, int]:
        """Check for transactions at unusual hours (1 AM - 5 AM)"""
        try:
            timestamp = datetime.fromisoformat(txn["timestamp"])
            hour = timestamp.hour
            
            if self.SUSPICIOUS_HOUR_START <= hour <= self.SUSPICIOUS_HOUR_END:
                return "UNUSUAL_HOUR", 15
        except (ValueError, KeyError):
            logger.error(f"Invalid timestamp format: {txn.get('timestamp')}")
        
        return None, 0
    
    def _check_velocity(self, txn: Dict) -> tuple[str, int]:
        """Check for multiple transactions in short time (velocity check)"""
        user_id = txn["user_id"]
        current_time = datetime.fromisoformat(txn["timestamp"])
        
        # Initialize user history if not exists
        if user_id not in self.user_transaction_history:
            self.user_transaction_history[user_id] = []
        
        # Get recent transactions for this user
        recent_txns = self.user_transaction_history[user_id]
        
        # Count transactions in last 5 minutes
        recent_count = 0
        for past_txn_time in recent_txns:
            time_diff = (current_time - past_txn_time).total_seconds()
            if time_diff <= self.RAPID_TRANSACTION_WINDOW:
                recent_count += 1
        
        # Add current transaction to history
        self.user_transaction_history[user_id].append(current_time)
        
        # Keep only last 10 transactions per user (memory management)
        if len(self.user_transaction_history[user_id]) > 10:
            self.user_transaction_history[user_id] = self.user_transaction_history[user_id][-10:]
        
        # Flag if more than 3 transactions in 5 minutes
        if recent_count >= 3:
            return "RAPID_TRANSACTIONS", 25
        elif recent_count == 2:
            return "FREQUENT_TRANSACTIONS", 10
        
        return None, 0
    
    def _check_round_amount(self, txn: Dict) -> tuple[str, int]:
        """Check for suspiciously round amounts (e.g., exactly 1000, 5000)"""
        amount = txn["amount"]
        
        # Check if amount is exactly divisible by 1000
        if amount >= 1000 and amount % 1000 == 0:
            return "ROUND_AMOUNT", 10
        
        return None, 0
    
    def _check_missing_profile(self, txn: Dict) -> tuple[str, int]:
        """Check if user profile is missing or incomplete"""
        user = txn.get("user_profile", {})
        
        if not user:
            return "MISSING_USER_PROFILE", 20
        
        # Check for incomplete profile
        required_fields = ["name", "home_country", "home_city"]
        missing_fields = [field for field in required_fields if not user.get(field)]
        
        if missing_fields:
            return "INCOMPLETE_USER_PROFILE", 15
        
        return None, 0
    
    def get_risk_level(self, risk_score: int) -> str:
        """
        Convert numeric risk score to risk level category
        
        Args:
            risk_score: Calculated risk score
        
        Returns:
            Risk level: LOW, MEDIUM, HIGH, or CRITICAL
        """
        if risk_score >= 60:
            return "CRITICAL"
        elif risk_score >= 40:
            return "HIGH"
        elif risk_score >= 20:
            return "MEDIUM"
        else:
            return "LOW"