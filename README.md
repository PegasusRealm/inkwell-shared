# InkWell Shared Backend

Shared Firebase backend code for InkWell web and mobile apps.

## Contents
- Cloud Functions (API endpoints, triggers)
- Firestore security rules
- Firestore indexes
- Firebase configuration

## Usage
This repository is used as a Git submodule in both web and mobile projects.

## Deployment

```bash
# Deploy all
npm run deploy

# Deploy functions only
npm run deploy:functions

# Deploy rules only
npm run deploy:rules
```
