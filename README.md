# Fraud Detection System

## Introduction  
Le projet vise à développer un système de détection de fraude pour la société **FinTech Innovations**. Il permet d'analyser en quasi temps réel les transactions et de détecter les activités suspectes tout en minimisant les fausses alertes.  

## Fonctionnalités  
- **Collecte et Intégration des Données** : Extraction et transformation des données de transaction, client et externes.  
- **Stockage et Gestion des Données**  :  
  - Utilisation de Kafka pour le streaming des transactions.  
  - Traitement des données  avec Apache Flink.  
- **Détection de Fraude Basée sur les Règles** :  
  - Analyse des transactions avec Flink SQL.  
  - Identification des transactions suspectes (montants élevés, fréquence anormale, localisation inhabituelle, liste noire, etc.).  
- **Déploiement**:  
  - Orchestration des tâches avec **Airflow DAG**.
  - Intégration CI/CD via **GitHub Actions**. 

## Technologies Utilisées  
- **Python** 
- **Apache Flink** 
- **Apache Kafka** (streaming des transactions)  
- **Apache Airflow** (orchestration des tâches)
- **GitHub Actions** (CI/CD) 

## Installation  
1. Cloner le projet :  
   ```bash
   git clone https://github.com/rahiqi-al/fraud.git  
   cd fraud-detection  
   ```  
2. Installer les dépendances Python :  
   ```bash
   pip install -r requirements.txt  
   ```  
3. Configurer Apache Kafka, Apache Flink et Apache Airflow.  

## Contact  
Pour toute question ou suggestion, veuillez contacter ALI RAHIQI à ali123rahiqi@gmail.com.
