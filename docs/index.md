---
layout: home
---

# Welcome to Casper's Kitchens

Casper's Kitchens unifies streaming, ETL, BI, Gen AI, Lakebase and Apps into one cohesive Databricks narrative — a fully working ghost kitchen business you can deploy, explore, and extend.

## Latest Posts

{% for post in site.posts limit:5 %}
  <div>
    <h3><a href="{{ post.url | relative_url }}">{{ post.title }}</a></h3>
    <p>{{ post.date | date: "%B %d, %Y" }}</p>
    <p>{{ post.excerpt }}</p>
  </div>
{% endfor %}