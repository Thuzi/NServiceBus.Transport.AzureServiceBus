namespace NServiceBus.Transport.AzureServiceBus
{
    static class SettingsKeys
    {
        const string Base = "AzureServiceBus.";
        public const string TopicName = Base + nameof(TopicName);
        public const string MaximumSizeInGB = Base + nameof(MaximumSizeInGB);
        public const string EnablePartitioning = Base + nameof(EnablePartitioning);
        public const string PrefetchMultiplier = Base + nameof(PrefetchMultiplier);
        public const string PrefetchCount = Base + nameof(PrefetchCount);
        public const string TimeToWaitBeforeTriggeringCircuitBreaker = Base + nameof(TimeToWaitBeforeTriggeringCircuitBreaker);
        public const string SubscriptionNameFactory = Base + nameof(SubscriptionNameFactory);
        public const string SqlExpressionFactory = Base + nameof(SqlExpressionFactory);
        public const string RuleNameFactory = Base + nameof(RuleNameFactory);
        public const string TransportType = Base + nameof(TransportType);
        public const string CustomTokenProvider = Base + nameof(CustomTokenProvider);
    }
}