use movie_pipeline;
CREATE TABLE IF NOT EXISTS movies_data (movieId INT PRIMARY KEY, title VARCHAR(255),genres VARCHAR(255),director VARCHAR(255), release_date date , rating FLOAT );
select*from movies_data limit 5;

