from flask import Flask, render_template, request, redirect, url_for, flash
import snowflake.connector
import logging

app = Flask(__name__)
app.secret_key = 'e44920a1fd9e49321f61cb8b24a758c7'

# Enable logging
logging.basicConfig(level=logging.DEBUG)

# Snowflake connection
def get_snowflake_connection():
    try:
        logging.debug("Attempting to connect to Snowflake with the following details:")
        logging.debug(f"user: {'dramanagari32906'}")
        logging.debug(f"password: {'<hidden>'}")
        logging.debug(f"account: {'kh18270.us-east-2.aws'}")
        logging.debug(f"role: {'ACCOUNTADMIN'}")
        logging.debug(f"warehouse: {'COMPUTE_WH'}")
        logging.debug(f"database: {'BENCH_SALES'}")
        logging.debug(f"schema: {'PUBLIC'}")

        conn = snowflake.connector.connect(
            user='dramanagari32906',
            password='Ramdurga1985@',
            account='kh18270.us-east-2.aws',
            role='ACCOUNTADMIN',
            warehouse='COMPUTE_WH',
            database='BENCH_SALES',
            schema='PUBLIC'
        )
        logging.debug("Snowflake connection established successfully.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        return None

@app.route('/')
def index():
    conn = get_snowflake_connection()
    if not conn:
        flash("Unable to connect to the database. Please check your credentials.", "error")
        return render_template('index.html', technologies=[])
    
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT TECHNOLOGY FROM CANDIDATES')
        technologies = cursor.fetchall()
        cursor.close()
        conn.close()
        return render_template('index.html', technologies=technologies)
    except Exception as e:
        logging.error(f"Error fetching technologies: {e}")
        flash("Error fetching data from the database.", "error")
        return render_template('index.html', technologies=[])

@app.route('/technology/<technology>')
def technology(technology):
    conn = get_snowflake_connection()
    if not conn:
        flash("Unable to connect to the database. Please check your credentials.", "error")
        return render_template('technology.html', technology=technology, candidates=[])
    
    try:
        cursor = conn.cursor()
        logging.debug(f'Executing query: SELECT CANDIDATE_FULL_LEGAL_NAME FROM CANDIDATES WHERE TECHNOLOGY = \'{technology}\'')
        cursor.execute(f"SELECT CANDIDATE_FULL_LEGAL_NAME FROM CANDIDATES WHERE TECHNOLOGY = '{technology}'")
        candidates = cursor.fetchall()
        cursor.close()
        conn.close()
        return render_template('technology.html', technology=technology, candidates=candidates)
    except Exception as e:
        logging.error(f"Error fetching data for technology {technology}: {e}")
        flash(f"Error fetching data for technology {technology} from the database.", "error")
        return render_template('technology.html', technology=technology, candidates=[])

@app.route('/candidate/<candidate>')
def candidate(candidate):
    conn = get_snowflake_connection()
    if not conn:
        flash("Unable to connect to the database. Please check your credentials.", "error")
        return render_template('candidate.html', candidate=candidate, details={}, technology="")
    
    try:
        cursor = conn.cursor()
        logging.debug(f'Executing query: SELECT * FROM CANDIDATES WHERE CANDIDATE_FULL_LEGAL_NAME = \'{candidate}\'')
        cursor.execute(f"SELECT * FROM CANDIDATES WHERE CANDIDATE_FULL_LEGAL_NAME = '{candidate}'")
        details = cursor.fetchone()
        logging.debug(f"Details fetched for candidate {candidate}: {details}")
        technology = details[1]  # Assuming the second column is the technology
        cursor.close()
        conn.close()
        return render_template('candidate.html', candidate=candidate, details=details, technology=technology)
    except Exception as e:
        logging.error(f"Error fetching data for candidate {candidate}: {e}")
        flash(f"Error fetching data for candidate {candidate} from the database.", "error")
        return render_template('candidate.html', candidate=candidate, details={}, technology="")

if __name__ == '__main__':
    app.run(debug=True)
