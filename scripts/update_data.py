#!/usr/bin/env python3
"""
Script para buscar dados da planilha Google Sheets e gerar arquivo JSON
Para o contador de inscrições PasCom 2025
"""

import os
import json
import pandas as pd
import requests
from collections import Counter
from datetime import datetime

def log_message(message):
    """Função para log com timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_sheet_data(sheet_id):
    """
    Busca dados da planilha Google Sheets via CSV público
    """
    try:
        # URL para exportar como CSV (não precisa de API Key)
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        
        log_message(f"🔍 Buscando dados da planilha: {sheet_id}")
        
        # Faz a requisição
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()
        
        # Salva temporariamente como CSV
        with open('temp_data.csv', 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        # Lê o CSV com pandas
        df = pd.read_csv('temp_data.csv')
        
        # Remove arquivo temporário
        os.remove('temp_data.csv')
        
        log_message(f"✅ Dados carregados: {len(df)} linhas encontradas")
        return df
        
    except Exception as e:
        log_message(f"❌ Erro ao buscar dados: {str(e)}")
        return None

def process_paroquia_data(df):
    """
    Processa os dados e conta inscrições por paróquia
    """
    if df is None or df.empty:
        log_message("⚠️ Nenhum dado para processar")
        return {}
    
    try:
        # Identifica a coluna das paróquias
        # Procura por colunas que contenham "paróquia" ou "cidade" no nome
        paroquia_column = None
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ['paróquia', 'paroquia', 'cidade', 'qual sua']):
                paroquia_column = col
                break
        
        if paroquia_column is None:
            # Se não encontrou, usa a 5ª coluna (índice 4) - baseado na estrutura vista
            if len(df.columns) >= 5:
                paroquia_column = df.columns[4]
            else:
                log_message("❌ Não foi possível identificar a coluna das paróquias")
                return {}
        
        log_message(f"📍 Usando coluna: {paroquia_column}")
        
        # Remove linhas vazias e conta ocorrências
        paroquias_list = df[paroquia_column].dropna().tolist()
        
        # Remove cabeçalho se ainda estiver presente
        paroquias_list = [p for p in paroquias_list if str(p).strip() and 'paróquia' in str(p).lower()]
        
        # Conta as ocorrências
        paroquia_counts = Counter(paroquias_list)
        
        # Converte para dicionário normal
        result = dict(paroquia_counts)
        
        log_message(f"📊 Processamento concluído: {len(result)} paróquias encontradas")
        log_message(f"📈 Total de inscrições: {sum(result.values())}")
        
        # Log das paróquias encontradas
        for paroquia, count in sorted(result.items(), key=lambda x: x[1], reverse=True):
            log_message(f"  • {paroquia}: {count} inscrições")
        
        return result
        
    except Exception as e:
        log_message(f"❌ Erro ao processar dados: {str(e)}")
        return {}

def load_existing_data():
    """
    Carrega dados existentes do arquivo JSON
    """
    try:
        if os.path.exists('dados.json'):
            with open('dados.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                log_message(f"📂 Dados existentes carregados: {len(data)} paróquias")
                return data
    except Exception as e:
        log_message(f"⚠️ Erro ao carregar dados existentes: {str(e)}")
    
    return {}

def save_data(data):
    """
    Salva os dados no arquivo JSON
    """
    try:
        # Ordena os dados por número de inscrições (decrescente)
        sorted_data = dict(sorted(data.items(), key=lambda x: x[1], reverse=True))
        
        with open('dados.json', 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
        
        log_message(f"💾 Dados salvos: {len(sorted_data)} paróquias")
        return True
        
    except Exception as e:
        log_message(f"❌ Erro ao salvar dados: {str(e)}")
        return False

def merge_data(existing_data, new_data):
    """
    Combina dados existentes com novos dados
    Mantém contadores manuais se forem maiores
    """
    merged_data = existing_data.copy()
    
    for paroquia, count in new_data.items():
        if paroquia in merged_data:
            # Mantém o maior valor (permite ajustes manuais)
            merged_data[paroquia] = max(merged_data[paroquia], count)
        else:
            # Nova paróquia
            merged_data[paroquia] = count
            log_message(f"🆕 Nova paróquia adicionada: {paroquia}")
    
    return merged_data

def main():
    """
    Função principal
    """
    log_message("🚀 Iniciando atualização dos dados...")
    
    # Pega ID da planilha do ambiente ou usa padrão
    sheet_id = os.environ.get('SHEET_ID', '1Q7uu8xyc7qde84epVGSO8TJViuDO4mvUnie-dwul9q0')
    
    if not sheet_id:
        log_message("❌ SHEET_ID não definido!")
        exit(1)
    
    # Busca dados da planilha
    df = get_sheet_data(sheet_id)
    new_data = process_paroquia_data(df)
    
    if not new_data:
        log_message("⚠️ Nenhum dado novo encontrado, mantendo dados existentes")
        exit(0)
    
    # Carrega dados existentes
    existing_data = load_existing_data()
    
    # Combina dados
    final_data = merge_data(existing_data, new_data)
    
    # Salva dados atualizados
    if save_data(final_data):
        log_message("✅ Atualização concluída com sucesso!")
        
        # Exibe estatísticas finais
        total_inscricoes = sum(final_data.values())
        total_paroquias = len([p for p, c in final_data.items() if c > 0])
        log_message(f"📊 Estatísticas finais:")
        log_message(f"   Total de inscrições: {total_inscricoes}")
        log_message(f"   Paróquias com inscrições: {total_paroquias}")
        log_message(f"   Média por paróquia: {total_inscricoes/total_paroquias:.1f}" if total_paroquias > 0 else "   Média: 0")
    else:
        log_message("❌ Falha ao salvar dados")
        exit(1)

if __name__ == "__main__":
    main()