"""
Tests for configuration helper functions
"""

from unittest.mock import patch

from wickdata.models.config import ExchangeConfig
from wickdata.utils.config_helpers import (
    create_binance_config,
    create_bybit_config,
    create_coinbase_config,
    create_kraken_config,
)


class TestBinanceConfig:
    """Test Binance configuration helper"""

    def test_create_binance_config_basic(self):
        """Test creating basic Binance config"""
        config = create_binance_config()

        assert isinstance(config, ExchangeConfig)
        assert config.exchange == "binance"
        assert config.api_key is None
        assert config.secret is None
        assert config.enable_rate_limit is True
        assert config.options["adjustForTimeDifference"] is True
        assert config.options["recvWindow"] == 60000

    def test_create_binance_config_with_credentials(self):
        """Test creating Binance config with API credentials"""
        config = create_binance_config(api_key="test_key", secret="test_secret")

        assert config.api_key == "test_key"
        assert config.secret == "test_secret"

    def test_create_binance_config_testnet(self):
        """Test creating Binance config for testnet"""
        config = create_binance_config(testnet=True)

        assert config.options["test"] is True
        assert "urls" in config.options
        assert "api" in config.options["urls"]
        assert config.options["urls"]["api"]["public"] == "https://testnet.binance.vision/api"
        assert config.options["urls"]["api"]["private"] == "https://testnet.binance.vision/api"

    def test_create_binance_config_with_custom_options(self):
        """Test creating Binance config with custom options"""
        config = create_binance_config(custom_option="value", another_option=123)

        assert config.options["custom_option"] == "value"
        assert config.options["another_option"] == 123
        # Default options should still be present
        assert config.options["adjustForTimeDifference"] is True
        assert config.options["recvWindow"] == 60000

    def test_create_binance_config_override_defaults(self):
        """Test overriding default options"""
        config = create_binance_config(recvWindow=30000, adjustForTimeDifference=False)

        assert config.options["recvWindow"] == 30000
        assert config.options["adjustForTimeDifference"] is False

    def test_create_binance_config_testnet_with_credentials(self):
        """Test creating testnet config with credentials"""
        config = create_binance_config(api_key="test_key", secret="test_secret", testnet=True)

        assert config.api_key == "test_key"
        assert config.secret == "test_secret"
        assert config.options["test"] is True
        assert "urls" in config.options


class TestCoinbaseConfig:
    """Test Coinbase configuration helper"""

    def test_create_coinbase_config_basic(self):
        """Test creating basic Coinbase config"""
        config = create_coinbase_config()

        assert isinstance(config, ExchangeConfig)
        assert config.exchange == "coinbase"
        assert config.api_key is None
        assert config.secret is None
        assert config.password is None
        assert config.enable_rate_limit is True

    def test_create_coinbase_config_with_credentials(self):
        """Test creating Coinbase config with full credentials"""
        config = create_coinbase_config(
            api_key="test_key", secret="test_secret", passphrase="test_passphrase"
        )

        assert config.api_key == "test_key"
        assert config.secret == "test_secret"
        assert config.password == "test_passphrase"

    def test_create_coinbase_config_sandbox(self):
        """Test creating Coinbase config for sandbox"""
        config = create_coinbase_config(sandbox=True)

        assert "urls" in config.options
        assert config.options["urls"]["api"] == "https://api-public.sandbox.exchange.coinbase.com"

    def test_create_coinbase_config_with_custom_options(self):
        """Test creating Coinbase config with custom options"""
        config = create_coinbase_config(timeout=30000, custom_header="value")

        assert config.options["timeout"] == 30000
        assert config.options["custom_header"] == "value"

    def test_create_coinbase_config_sandbox_with_credentials(self):
        """Test creating sandbox config with credentials"""
        config = create_coinbase_config(
            api_key="test_key", secret="test_secret", passphrase="test_passphrase", sandbox=True
        )

        assert config.api_key == "test_key"
        assert config.secret == "test_secret"
        assert config.password == "test_passphrase"
        assert "urls" in config.options


class TestKrakenConfig:
    """Test Kraken configuration helper"""

    @patch("wickdata.utils.config_helpers.time")
    def test_create_kraken_config_basic(self, mock_time):
        """Test creating basic Kraken config"""
        mock_time.time.return_value = 1234567.890

        config = create_kraken_config()

        assert isinstance(config, ExchangeConfig)
        assert config.exchange == "kraken"
        assert config.api_key is None
        assert config.secret is None
        assert config.enable_rate_limit is True
        assert "nonce" in config.options

        # Test nonce function
        nonce_func = config.options["nonce"]
        assert callable(nonce_func)
        assert nonce_func() == 1234567890

    def test_create_kraken_config_with_credentials(self):
        """Test creating Kraken config with API credentials"""
        config = create_kraken_config(api_key="test_key", secret="test_secret")

        assert config.api_key == "test_key"
        assert config.secret == "test_secret"

    @patch("wickdata.utils.config_helpers.time")
    def test_create_kraken_config_with_custom_options(self, mock_time):
        """Test creating Kraken config with custom options"""
        mock_time.time.return_value = 1234567.890

        config = create_kraken_config(tier="pro", otp="123456")

        assert config.options["tier"] == "pro"
        assert config.options["otp"] == "123456"
        # Default nonce should still be present
        assert "nonce" in config.options

    @patch("wickdata.utils.config_helpers.time")
    def test_create_kraken_config_override_nonce(self, mock_time):
        """Test overriding default nonce function"""

        def custom_nonce():
            return 999999999

        config = create_kraken_config(nonce=custom_nonce)

        assert config.options["nonce"] == custom_nonce
        assert config.options["nonce"]() == 999999999

    @patch("wickdata.utils.config_helpers.time")
    def test_kraken_nonce_increments(self, mock_time):
        """Test that nonce function produces incrementing values"""
        # Simulate time passing
        mock_time.time.side_effect = [1234567.890, 1234568.000, 1234568.100]

        config = create_kraken_config()
        nonce_func = config.options["nonce"]

        nonce1 = nonce_func()
        nonce2 = nonce_func()
        nonce3 = nonce_func()

        assert nonce1 == 1234567890
        assert nonce2 == 1234568000
        assert nonce3 == 1234568100
        assert nonce1 < nonce2 < nonce3


class TestBybitConfig:
    """Test Bybit configuration helper"""

    def test_create_bybit_config_basic(self):
        """Test creating basic Bybit config"""
        config = create_bybit_config()

        assert isinstance(config, ExchangeConfig)
        assert config.exchange == "bybit"
        assert config.api_key is None
        assert config.secret is None
        assert config.enable_rate_limit is True
        assert config.options["adjustForTimeDifference"] is True

    def test_create_bybit_config_with_credentials(self):
        """Test creating Bybit config with API credentials"""
        config = create_bybit_config(api_key="test_key", secret="test_secret")

        assert config.api_key == "test_key"
        assert config.secret == "test_secret"

    def test_create_bybit_config_testnet(self):
        """Test creating Bybit config for testnet"""
        config = create_bybit_config(testnet=True)

        assert "urls" in config.options
        assert "api" in config.options["urls"]
        assert config.options["urls"]["api"]["public"] == "https://api-testnet.bybit.com"
        assert config.options["urls"]["api"]["private"] == "https://api-testnet.bybit.com"

    def test_create_bybit_config_with_custom_options(self):
        """Test creating Bybit config with custom options"""
        config = create_bybit_config(recv_window=5000, enable_unified_margin=True)

        assert config.options["recv_window"] == 5000
        assert config.options["enable_unified_margin"] is True
        # Default options should still be present
        assert config.options["adjustForTimeDifference"] is True

    def test_create_bybit_config_override_defaults(self):
        """Test overriding default options"""
        config = create_bybit_config(adjustForTimeDifference=False)

        assert config.options["adjustForTimeDifference"] is False

    def test_create_bybit_config_testnet_with_credentials(self):
        """Test creating testnet config with credentials"""
        config = create_bybit_config(api_key="test_key", secret="test_secret", testnet=True)

        assert config.api_key == "test_key"
        assert config.secret == "test_secret"
        assert "urls" in config.options


class TestConfigIntegration:
    """Test configuration helpers integration"""

    def test_all_configs_return_exchange_config(self):
        """Test that all helpers return ExchangeConfig instances"""
        configs = [
            create_binance_config(),
            create_coinbase_config(),
            create_kraken_config(),
            create_bybit_config(),
        ]

        for config in configs:
            assert isinstance(config, ExchangeConfig)
            assert config.enable_rate_limit is True

    def test_all_configs_accept_credentials(self):
        """Test that all helpers accept API credentials"""
        api_key = "test_key"
        secret = "test_secret"

        binance_config = create_binance_config(api_key=api_key, secret=secret)
        coinbase_config = create_coinbase_config(api_key=api_key, secret=secret)
        kraken_config = create_kraken_config(api_key=api_key, secret=secret)
        bybit_config = create_bybit_config(api_key=api_key, secret=secret)

        configs = [binance_config, coinbase_config, kraken_config, bybit_config]

        for config in configs:
            assert config.api_key == api_key
            assert config.secret == secret

    def test_all_configs_accept_custom_options(self):
        """Test that all helpers accept custom options"""
        custom_option = "custom_value"

        configs = [
            create_binance_config(custom=custom_option),
            create_coinbase_config(custom=custom_option),
            create_kraken_config(custom=custom_option),
            create_bybit_config(custom=custom_option),
        ]

        for config in configs:
            assert config.options["custom"] == custom_option

    def test_testnet_sandbox_configs(self):
        """Test testnet/sandbox configurations"""
        binance_testnet = create_binance_config(testnet=True)
        coinbase_sandbox = create_coinbase_config(sandbox=True)
        bybit_testnet = create_bybit_config(testnet=True)

        # Binance testnet
        assert binance_testnet.options.get("test") is True
        assert "urls" in binance_testnet.options

        # Coinbase sandbox
        assert "urls" in coinbase_sandbox.options

        # Bybit testnet
        assert "urls" in bybit_testnet.options

    def test_exchange_names_correct(self):
        """Test that exchange names are set correctly"""
        assert create_binance_config().exchange == "binance"
        assert create_coinbase_config().exchange == "coinbase"
        assert create_kraken_config().exchange == "kraken"
        assert create_bybit_config().exchange == "bybit"
