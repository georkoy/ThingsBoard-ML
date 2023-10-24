import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from datetime import datetime, timedelta
import joblib
from sklearn.metrics import mean_squared_log_error, mean_squared_error, median_absolute_error, explained_variance_score
import sys
import psycopg2
from psycopg2 import OperationalError


def model_metrics(X_train ,y_train_trimmed,y_std,y_mean):
    # Split the data into training and testing sets
    X_train_split, X_test_split, y_train_split, y_test_split = train_test_split(
        X_train, y_train_trimmed, test_size=0.2, shuffle=False
    )

    # Create the XGBoost model with regularization (Step 1)
    model = XGBRegressor()#eta=0.1,reg_alpha=0.1, reg_lambda=0.1,max_depth=12)
    model.fit(X_train_split, y_train_split)

    # Generate predictions for the testing set
    y_pred_split = model.predict(X_test_split)

    # Denormalize the predictions
    y_pred_split = (y_pred_split * y_std) + y_mean
    y_actual_split = (y_test_split * y_std) + y_mean

    # Calculate and print metrics (MSE, MAE, R2) for the testing set
    mse_split = mean_squared_error(y_actual_split, y_pred_split)
    mae_split = mean_absolute_error(y_actual_split, y_pred_split)
    r2_split = r2_score(y_actual_split, y_pred_split)
    medae_split = median_absolute_error(y_actual_split, y_pred_split)
    explained_var_score = explained_variance_score(y_actual_split, y_pred_split)
    print('Metrics on testing set:')
    print('Mean Squared Error (MSE): {:.2f}'.format(mse_split))
    print('Mean Absolute Error (MAE): {:.2f}'.format(mae_split))
    print('R-squared (R2): {:.2f}'.format(r2_split))
    print('Median Absolute Error (MedAE): {:.2f}'.format(medae_split))
    print('Explained Variance Score: {:.2f}'.format(explained_var_score))

def dbdata(predictions,next24,target_value):
	try:
	# Database connection parameters
		db_params = {
		'dbname': 'thingsboard',
		'user': 'postgres',
		'password': 'kH7apYLZAjTEaa3iw1dL',
		'host': 'localhost',
		'port': '5432'
		}
	# Establish a connection to the database
		conn = psycopg2.connect(**db_params)
        # Check if the connection is successful
		if conn:
		#print("Database connection successful!")
			cursor=conn.cursor()
			for predictions, date in zip(predictions,next24):
				gethour=date.strftime('%H')
				update_query = f'''UPDATE predictions SET {target_value}={predictions} WHERE date='{gethour}';'''
				cursor.execute(update_query)
		conn.commit()

        # Close the connection
		cursor.close()
		conn.close()
	

	except psycopg2.OperationalError as e:
		print("Error:", e)
		print("Database connection failed.")


def data_train(target_value):
   # model_final=joblib.load('testsave.sav')
    # Load the dataset with the correct date format
    df = pd.read_csv('data.csv', parse_dates=['Date'], date_format='%d/%m/%y %H:%M')

    # Drop rows with missing values
    df.dropna(inplace=True)

    # Extract the target variable
    y_train = df[target_value].values
    # Normalize the target variable
    y_mean = np.mean(y_train)
    y_std = np.std(y_train)
    y_train = (y_train - y_mean) / y_std

    # Define the number of previous time steps to consider
    lookback = 24  # Number of time steps to look back

    # Create the input features for the XGBoost model
    X_train = []
    y_train_trimmed = y_train[lookback:]  # Trim the target variable to match X_train length
    for i in range(lookback, len(y_train)):
        X_train.append(y_train[i - lookback:i])
    X_train = np.array(X_train)
    #model_metrics(X_train,y_train_trimmed,y_std,y_mean)

    # Train the XGBoost model using the entire dataset (Step 2)
    model_final = XGBRegressor()#eta=0.3,reg_alpha=0.1,reg_lambda=0.1)
    model_final.fit(X_train, y_train_trimmed)
    #joblib.dump(model_final,'testsave.sav')
    #print(df.iloc[-1])


    # Generate predictions for the next 24 hours using the last 24 hours of available data
    last_24_hours_data = y_train[-lookback:]
    predictions_final = []
    for _ in range(24):
        last_24_hours_reshaped = np.reshape(last_24_hours_data, (1, -1))
        y_pred_final = model_final.predict(last_24_hours_reshaped)
        predictions_final.append(y_pred_final[0])
        last_24_hours_data = np.append(last_24_hours_data[1:], y_pred_final)

    # Denormalize the predictions
    predictions_final = (np.array(predictions_final) * y_std) + y_mean

    # Display the predictions for the next 24 hours
    next_24_hours = pd.date_range(start=df['Date'].iloc[-1] + timedelta(hours=1), periods=24, freq='H')
    
    #for i, prediction in enumerate(predictions_final):
        #print('Prediction for {}: {:.2f}'.format(next_24_hours[i], prediction))
    dbdata(predictions_final,next_24_hours,target_value);

if __name__=='__main__':
	print("prediction.py is run")
	data_train("temperature")
	data_train("rain")
	data_train("pressure")
	data_train("humidity")
	data_train("windspeed")
	data_train("noise")
	data_train("co")
	data_train("no")
	data_train("no2")
	data_train("o3")
	data_train("so2")
	data_train("pm2_5")
	data_train("pm10")
	data_train("nh3")
	data_train("h2s")
	data_train("pm1")
	globals().clear()
	import gc
	gc.collect()
	for var in list(globals()):
		del globals()[var]
