from get_dataset import get_dataset
import great_expectations as gx
import pandas as pd
from great_expectations.checkpoint import SimpleCheckpoint

def validate_inventory_data():
    """
    Ejemplo completo de uso de Great Expectations para validar datos de inventario
    """
    
    # 1. Obtener los datos usando la función get_dataset
    query = """select top 50 *
    FROM pos.DailyInventoryHistoryDetails
    order by PosDailyInventoryHistoryDetailsKey desc;"""
    
    print("📊 Obteniendo datos...")
    df = get_dataset(query)
    print(f"✅ Datos obtenidos: {len(df)} filas, {len(df.columns)} columnas")
    print(f"Columnas: {list(df.columns)}")
    print("\n📋 Primeras 5 filas:")
    print(df.head())
    
    # 2. Crear el contexto de Great Expectations
    print("\n🔧 Configurando Great Expectations...")
    context = gx.get_context()
    
    # 3. Agregar fuente de datos pandas
    try:
        # Intentar obtener la fuente de datos existente
        data_source = context.data_sources.get("inventory_datasource")
        print("✅ Fuente de datos existente encontrada")
    except:
        # Crear nueva fuente de datos si no existe
        data_source = context.data_sources.add_pandas(
            name="inventory_datasource"
        )
        print("✅ Nueva fuente de datos creada")
    
    # 4. Agregar asset de datos
    try:
        data_asset = data_source.get_asset("inventory_data")
        print("✅ Asset de datos existente encontrado")
    except:
        data_asset = data_source.add_dataframe_asset(
            name="inventory_data"
        )
        print("✅ Nuevo asset de datos creado")
    
    # 5. Crear un Batch Request para nuestros datos
    batch_request = data_asset.build_batch_request(dataframe=df)
    
    # 6. Crear una Expectation Suite (conjunto de expectativas)
    try:
        suite = context.suites.get("inventory_suite")
        print("✅ Suite de expectativas existente encontrada")
    except:
        suite = context.suites.add(gx.ExpectationSuite(name="inventory_suite"))
        print("✅ Nueva suite de expectativas creada")
    
    # 7. Definir expectativas específicas para datos de inventario
    print("\n📏 Configurando expectativas de validación...")
    
    # Expectativas básicas de estructura de datos
    expectations = [
        # Verificar que el DataFrame no esté vacío
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
        
        # Verificar que ciertas columnas existan (ajustar según las columnas reales)
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=list(df.columns)
        ),
    ]
    
    # Agregar expectativas basadas en las columnas que realmente existen
    if 'PosDailyInventoryHistoryDetailsKey' in df.columns:
        expectations.extend([
            # La clave principal no debe ser nula
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="PosDailyInventoryHistoryDetailsKey"
            ),
            # La clave principal debe ser única
            gx.expectations.ExpectColumnValuesToBeUnique(
                column="PosDailyInventoryHistoryDetailsKey"
            ),
        ])
    
    # Agregar expectativas para columnas numéricas
    numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
    for col in numeric_columns:
        if col != 'PosDailyInventoryHistoryDetailsKey':  # Skip primary key
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column=col, type_="int64"
                )
            )
    
    # Agregar expectativas para columnas de fecha
    date_columns = df.select_dtypes(include=['datetime64']).columns
    for col in date_columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column=col, type_="datetime64"
            )
        )
    
    # Agregar todas las expectativas a la suite
    for expectation in expectations:
        suite.add_expectation(expectation)
    
    # 8. Crear un Validator para ejecutar las validaciones
    print("\n🔍 Ejecutando validaciones...")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )
    
    # 9. Ejecutar las validaciones
    validation_result = validator.validate()
    
    # 10. Mostrar resultados de la validación
    print(f"\n📊 Resultados de validación:")
    print(f"✅ Éxito general: {validation_result.success}")
    print(f"📈 Expectativas evaluadas: {validation_result.statistics['evaluated_expectations']}")
    print(f"✅ Expectativas exitosas: {validation_result.statistics['successful_expectations']}")
    print(f"❌ Expectativas fallidas: {validation_result.statistics['unsuccessful_expectations']}")
    print(f"📊 Porcentaje de éxito: {validation_result.statistics['success_percent']:.2f}%")
    
    # 11. Detallar expectativas fallidas si las hay
    if not validation_result.success:
        print("\n❌ Expectativas que fallaron:")
        for result in validation_result.results:
            if not result.success:
                print(f"  - {result.expectation_config.expectation_type}")
                if 'partial_unexpected_list' in result.result:
                    print(f"    Valores inesperados: {result.result['partial_unexpected_list']}")
    
    # 12. Opcional: Crear un Checkpoint para automatizar validaciones futuras
    try:
        checkpoint = context.checkpoints.get("inventory_checkpoint")
        print("\n✅ Checkpoint existente encontrado")
    except:
        checkpoint_config = {
            "name": "inventory_checkpoint",
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "inventory_datasource",
                        "data_asset_name": "inventory_data"
                    },
                    "expectation_suite_name": "inventory_suite"
                }
            ]
        }
        
        checkpoint = context.checkpoints.add(gx.checkpoint.Checkpoint(**checkpoint_config))
        print("\n✅ Checkpoint creado para validaciones futuras")
    
    # 13. Guardar configuración
    context.save_expectation_suite(suite)
    
    print(f"\n🎉 Validación completada!")
    print(f"💾 Configuración guardada para uso futuro")
    
    return validation_result, df

if __name__ == "__main__":
    # Ejecutar el ejemplo
    validation_result, df = validate_inventory_data()
    
    # Información adicional sobre el DataFrame
    print(f"\n📊 Información adicional del DataFrame:")
    print(f"📏 Forma: {df.shape}")
    print(f"🔢 Tipos de datos:")
    print(df.dtypes)
    print(f"\n📈 Estadísticas básicas:")
    print(df.describe())