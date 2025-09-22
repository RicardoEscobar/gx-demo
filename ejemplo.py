if __name__ == "__main__":
    # Ejemplo de query
    query = """select top 50 *
    FROM pos.DailyInventoryHistoryDetails
    order by PosDailyInventoryHistoryDetailsKey desc;"""
    
    df = get_dataset(query)
    print(df.head())