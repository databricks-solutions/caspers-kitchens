# Casper's Kitchens - Documentation

This documentation provides comprehensive guidance for understanding and working with the Casper's Kitchens ghost kitchen data platform.

## 📋 Documentation Overview

### 🎯 [Dataflow Architecture Diagram](./dataflow-diagram.md)
Complete visual overview of the data architecture showing:
- Event sources and data ingestion
- Medallion architecture layers (Bronze → Silver → Gold)
- Applications and consumption patterns
- Data lineage and dependencies

### 🔧 [Technical Reference](./technical-reference.md)
Detailed technical specifications including:
- Complete table schemas and data types
- Transformation logic and SQL implementations
- Configuration parameters and settings

### 👨‍💻 [Developer Onboarding Guide](./developer-onboarding.md)
Step-by-step guide for new developers covering:
- Architecture overview and key concepts
- Essential files and code walkthrough
- Common development tasks and patterns
- SQL queries for monitoring and validation

### 🎨 Visual Dataflow Diagrams
Complete dataflow visualization available in multiple formats:
- **[PNG Image](./images/dataflow-diagram.png)** - Standard resolution with dark theme
- **[High-Res PNG](./images/dataflow-diagram-hd.png)** - High resolution for presentations  
- **[SVG Vector](./images/dataflow-diagram.svg)** - Scalable vector format
- **[Mermaid Source](./dataflow-diagram.mermaid)** - Source code for modifications

## 🚀 Quick Navigation

### For New Developers
1. Start with the [Developer Onboarding Guide](./developer-onboarding.md)
2. Review the [Dataflow Architecture](./dataflow-diagram.md)
3. Reference the [Technical Specifications](./technical-reference.md) as needed

### For Data Engineers
1. Examine the [Technical Reference](./technical-reference.md) for implementation details
2. Use the [Dataflow Diagram](./dataflow-diagram.md) to understand data lineage
3. Follow the [Developer Guide](./developer-onboarding.md) for common tasks

### For Architects
1. Review the [Dataflow Architecture](./dataflow-diagram.md) for system design
2. Check the [Technical Reference](./technical-reference.md) for scalability details
3. Use the [Mermaid Diagram](./dataflow-diagram.mermaid) for presentations

## 🏗️ Architecture Summary

Casper's Kitchens implements a modern data platform with:

- **Real-time Event Processing**: CloudFiles streaming from ghost kitchen operations
- **Medallion Architecture**: Bronze → Silver → Gold data layers with Delta Live Tables
- **Streaming Intelligence**: ML-powered refund recommendations using LLMs
- **Operational Applications**: FastAPI web apps backed by Lakebase PostgreSQL
- **Business Intelligence**: Real-time dashboards and analytics

## 📊 Key Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Event Sources | Ghost kitchen operations | JSON events, GPS tracking |
| Bronze Layer | Raw event storage | Delta Live Tables, CloudFiles |
| Silver Layer | Clean operational data | Spark streaming, schema enforcement |
| Gold Layer | Business intelligence | Aggregations, time-series data |
| Streaming ML | Real-time recommendations | LLM integration, Spark streaming |
| Lakebase | Operational database | PostgreSQL, continuous sync |
| Applications | Human interfaces | FastAPI, React, REST APIs |

## 🔄 Data Flow Pattern

```
Ghost Kitchens → Events → Volume → Bronze → Silver → Gold → Apps
                                     ↓
                         Dimensional Data (Parquet)
                                     ↓
                         Streaming Intelligence (ML)
                                     ↓
                         Lakebase (PostgreSQL)
```

## 📈 Business Metrics

The platform tracks key business metrics including:

- **Order Performance**: Revenue, item counts, delivery times
- **Brand Analytics**: Sales by brand, menu performance
- **Location Intelligence**: Hourly performance by ghost kitchen
- **Operational Efficiency**: Refund rates, customer satisfaction
- **Real-time Monitoring**: Live order tracking, driver performance

## 🛠️ Development Workflow

1. **Understand**: Review architecture and data flow
2. **Explore**: Examine key code files and notebooks
3. **Develop**: Make changes to transformations or applications
4. **Test**: Validate using SQL queries and application UI
5. **Deploy**: Use pipeline orchestration for production changes
6. **Monitor**: Track performance and data quality

## 📚 Additional Resources

- **Main README**: `../README.md` - Project overview and quick start
- **Code Examples**: All notebooks include detailed comments
- **Configuration**: `../data/generator/configs/` - Simulation parameters
- **Applications**: `../apps/` - Web application source code
- **Pipelines**: `../pipelines/` - Data transformation logic

## 🤝 Contributing

When contributing to the documentation:

1. Keep diagrams and technical details in sync with code changes
2. Update the developer onboarding guide for new features
3. Maintain consistency in terminology and formatting
4. Test all code examples and SQL queries
5. Update the visual diagram when architecture changes
