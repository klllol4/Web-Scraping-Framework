flowchart TD
    classDef core fill:#f9f,stroke:#333,stroke-width:2px
    classDef module fill:#bbf,stroke:#333,stroke-width:1px
    classDef extension fill:#bfb,stroke:#333,stroke-width:1px

    User[Client Code] --> Core
    
    subgraph Framework
        Core[Core Orchestrator] :::core
        Core --> ConfigManager[Configuration Manager]
        Core --> Crawler
        Core --> Fetcher
        Core --> Parser
        Core --> Storage
        Core --> Compliance
        Core --> Monitoring[Monitoring & Logging]
        
        Crawler[Crawler Module] :::module
        Fetcher[Fetcher Module] :::module
        Parser[Parser Module] :::module
        Storage[Storage Module] :::module
        Compliance[Compliance Module] :::module
        
        Crawler --> UrlFrontier[URL Frontier]
        Crawler --> LinkExtractor[Link Extractor]
        
        Fetcher --> HttpClient[HTTP Client]
        Fetcher --> RetryPolicy[Retry Policy]
        Fetcher --> ProxyManager[Proxy Manager]
        Fetcher --> CaptchaSolver[CAPTCHA Solver]
        Fetcher --> RateLimiter[Rate Limiter]
        Fetcher --> SessionManager[Session Manager]
        Fetcher --> CacheManager[Cache Manager]
        
        Parser --> DomEngine[DOM Engine]
        Parser --> XPathEngine[XPath Engine]
        Parser --> CssEngine[CSS Selector Engine]
        Parser --> RegexEngine[Regex Engine]
        Parser --> SchemaValidator[Schema Validator]
        Parser --> Transformer[Data Transformer]
        
        Storage --> DbConnector[Database Connector]
        Storage --> FileSystem[File System]
        Storage --> StateManager[State Manager]
        
        Compliance --> RobotsTxt[Robots.txt Enforcer]
        Compliance --> UserAgentManager[User-Agent Manager]
        Compliance --> PolitenessManager[Politeness Policy]
        Compliance --> PrivacyFilter[Privacy Filter]
    end
    
    subgraph Extensions
        direction TB
        CsvExtractor[CSV Extractor] :::extension
        JsonExtractor[JSON Extractor] :::extension
        ImageProcessor[Image Processor] :::extension
        PdfExtractor[PDF Extractor] :::extension
        ApiConnector[API Connector] :::extension
    end
    
    Extensions --> Parser
    
    Fetcher --> External[External Services]
    Storage --> Databases[(Databases)]
    Monitoring --> Metrics[(Metrics Platform)]