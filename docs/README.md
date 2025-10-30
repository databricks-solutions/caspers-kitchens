# Casper's Kitchens Blog

Jekyll-based blog for the Casper's Kitchens project.

## Local Development

### Prerequisites

- Ruby 2.6+ with bundler

### Setup

```bash
cd docs
bundle config set --local path 'vendor/bundle'
bundle install
```

### Run Local Server

```bash
export PATH="/Users/$USER/.gem/ruby/2.6.0/bin:$PATH"
bundle exec jekyll serve --baseurl ""
```

Visit `http://localhost:4000`

## Writing Posts

### File Structure

Posts go in `_posts/` with format: `YYYY-MM-DD-title.md`

### Front Matter

```yaml
---
layout: post
title: "Your Title"
author: "Author Name"
description: "Brief description"
date: YYYY-MM-DD
categories: blog
tags: [Tag1, Tag2]
---
```

### Images

Place images in `assets/images/<post-specific-folder>/`

Reference in markdown:
```markdown
![Alt text]({{ site.baseurl }}/assets/images/folder/image.png)

*Optional caption in italics below image*
```

### Links

- Internal notebook links: Use full GitHub URLs
- README links: Use full GitHub URLs
- External links: Standard markdown

### Code Blocks

Wrap code containing `{{ }}` template syntax with raw tags:

```markdown
{% raw %}
```python
# code with {{ template }} syntax
```
{% endraw %}
```

## Deployment

Push to `blog` branch to deploy via GitHub Pages.
