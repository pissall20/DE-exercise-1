## How to set up and use the project

[Pipfile](Pipfile) is used to set up and manage the dependencies using pipenv. Navigate to the main directory and follow
below instructions

> - Use `pipenv install --dev` to install dependencies in a virtualenv.
> - Use `pipenv shell` to spawn a shell within the virtualenv.
> - The project is set up using Pipenv & Python 3.8


[Scripts](scripts) is provided with helpful scripts to fetch data, test, lint and tidy code. Pipenv is used to run the
scripts:

- `pipenv run fetch_data` - Downloads required data using shell script

- `pipenv run test` - Runs all unit test cases

- `pipenv run lint` - Check for potential errors

- `pipenv run tidy` - Tidy the code

[src/main.py](src/main.py) is provided as an entry point.

Your program will be invoked with `pipenv run python src/main.py /path/to/posts_file.json /path/to/votes_file.json`

### Final notes

- How did you meet the needs of a data scientist?
    - I had to analyse the dataset to validate the fields and the data quality, understand the relations between the two
      tables and to understand how to calculate the derived values of "what is the mean votes per post per week?"

- How did you ensure data quality?
    - Checking the data quality and integrity was a part of the analysis that was performed using pandas. Ensuring
      things like the primary key to be unique and date column is correct so that it can be converted without errors

- What would need to change for the solution scale to work with a 10TB dataset with new data arriving each day?
    - To manage a 10TB dataset we would require to read the data in batches and be inserted into a distributed database
      using a streaming/batch application. Big data frameworks like Spark/Hive/Pig can be used to query the data to
      answer the kind of questions in this exercise.
