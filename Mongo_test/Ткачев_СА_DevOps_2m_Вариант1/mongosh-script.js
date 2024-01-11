var conn = new Mongo();


const dbName = "movies";
var db = conn.getDB(dbName);


const fs = require('fs');

function writeResultToFile(taskNumber, result) {
    const fileName = `./answers/${taskNumber}.json`;

    fs.writeFile(fileName, JSON.stringify(result, null, 2), (err) => {
        if (err) {
            console.error(`Ошибка при записи в файл ${fileName}: ${err}`);
        } else {
            console.log(`Результат успешно записан в файл ${fileName}`);
        }
    });
}

// 1. Получите все фильмы из коллекции «movies», которые содержат полную информацию и имеют рейтинг зрителей выше 4 на Tomatoes

// Найтем все уникальные поля в коллекции

var uniqueFields = db.movies.aggregate([
    { $project: { _id: 0, fields: { $objectToArray: "$$ROOT" } } },
    { $unwind: "$fields" },
    { $group: { _id: null, allFields: { $addToSet: "$fields.k" } } },
    { $project: { _id: 0, allFields: 1 } }
]).next().allFields;

var moviesWithRatingAbove4 = db.movies.find({
    "tomatoes.viewer.rating": { $gt: 4 },
    "$and": uniqueFields.map(field => ({ [field]: { $exists: true } }))
});

task1 = moviesWithRatingAbove4.toArray();

// task1 = db.movies.find({
//     "tomatoes.viewer.rating": { $gt: 4 }
// }).toArray();

// 3. Найдите в коллекции «movies» в MongoDB все фильмы по названию, языкам, выпуску, режиссерам, сценаристам, наградам, году, жанрам, продолжительности показа, актерскому составу, странам, имеющим хотя бы одну номинацию.
task3 = db.movies.find({
    "awards.nominations": { $gt: 0 }
},
    {
        "title": 1,
        "languages": 1,
        "released": 1,
        "directors": 1,
        "writers": 1,
        "awards": 1,
        "year": 1,
        "genres": 1,
        "runtime": 1,
        "countries": 1,
        "cast": 1,
    }).toArray()


// // 5. Получить все фильмы с названием, языками, выпущенными, режиссерами, сценаристами и странами из коллекции «movies» в MongoDB, выпущенной 9 мая 1893 года.
task5 = db.movies.find({
    released: ISODate("1893-05-09T00:00:00.000Z")
},
    { title: 1, languages: 1, released: 1, directors: 1, writers: 1, countries: 1 }
).toArray();

// // 7. Найдите все фильмы с названием, языками, выпущенными, режиссерами, зрителями, сценаристами и странами из коллекции «movies» в MongoDB, которые имеют рейтинг зрителей не менее 3 и менее 4 на Tomatoes.
task7 = db.movies.find(
    { 'tomatoes.viewer.rating': { $gte: 3, $lt: 4 } },
    { title: 1, languages: 1, released: 1, directors: 1, 'tomatoes.viewer': 1, writers: 1, countries: 1 }
).toArray();

// // 9. Найдите все фильмы с названием, языками, полным сюжетом, выпущенным, режиссером, сценаристом и странами из коллекции «movies» в MongoDB, полный сюжет которого содержит слово «fire».
task9 = db.movies.find({
    fullplot: { $regex: /fire/i }
},
    { title: 1, languages: 1, fullplot: 1, released: 1, directors: 1, writers: 1, countries: 1 }
).toArray();

// // 11. Найдите в коллекции «movies» в MongoDB все фильмы с названием, языками, выпущенным, продолжительностью показа, режиссерами, сценаристами и странами, продолжительность которых составляет от 60 до 90 минут.
task11 = db.movies.find(
    { runtime: { $gte: 60, $lte: 90 } },
    { title: 1, languages: 1, released: 1, runtime: 1, directors: 1, writers: 1, countries: 1 }
).toArray();

// // 13. Найдите все фильмы из коллекции «movies» в MongoDB со средней продолжительностью просмотра фильмов, выпущенных в каждой стране.
task13 = db.movies.aggregate([
    {
        $match: { type: 'movie' }
    },
    {
        $group: { _id: '$countries', avgRuntime: { $avg: '$runtime' } }
    }
]).toArray();

// // 15. Найдите в коллекции «movies» в MongoDB фильмы, вышедшие в год с самым высоким средним рейтингом IMDb.
task15 = db.movies.aggregate([
    {
        $group: {
            _id: "$year",
            avgRating: { $avg: "$imdb.rating" },
            movies: { $push: "$$ROOT" }
        }
    },
    { $sort: { avgRating: -1 } },
    { $limit: 1 },
    { $unwind: "$movies" },
    { $replaceRoot: { newRoot: "$movies" } }
]).toArray();

// // 17. Напишите запрос в MongoDB, чтобы найти средний рейтинг IMDb для фильмов с разными рейтингами (например, «PG», «R», «G») из коллекции «movies».
task17 = db.movies.aggregate([
    {
        $group: { _id: "$rated", avgRating: { $avg: "$imdb.rating" } }
    }
]).toArray();

// // 19. Напишите запрос в MongoDB, чтобы найти фильм с самым высоким рейтингом IMDb и рейтингом зрителей на Tomatoes из коллекции «movies».
task19 = db.movies.aggregate([
    {
        $project: {
            _id: 1,
            title: 1,
            imdbRating: "$imdb.rating",
            tomatoRating: "$tomatoes.viewer.rating"
        }
    },
    { $sort: {imdbRating: -1, tomatoRating: -1} },
    { $limit: 1 }
]).toArray();


tasks = [
    task1,
    task3,
    task5,
    task7,
    task9,
    task11,
    task13,
    task15,
    task17,
    task19,
]

num = 1;

for (let i = 0; i < tasks.length; i++) {
    writeResultToFile(num, tasks[i]);
    num += 2;
}