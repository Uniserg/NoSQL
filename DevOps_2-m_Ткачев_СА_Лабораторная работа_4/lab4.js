var conn = new Mongo();


const dbName = "Restaurants";
var db = conn.getDB(dbName);

db.dropDatabase();
db.answers.drop();


const insertToAnswer = function (id, ans) {
    print(`Задание ${id} выполнено!`)
    db.answers.updateOne(
        { _id: id },
        { $set: { answer: ans } },
        { upsert: true }
    );
    return ans;
};

const { execSync } = require('child_process');


execSync(`mongoimport --db ${dbName} --collection restaurants --file ./restaurants.json`);
console.log('restaurants.json успешно импортирован');



// 1.	Напишите запрос MongoDB для отображения всех документов в коллекционных ресторанах. 
insertToAnswer(1, db.restaurants.find().toArray());

// 2.	Напишите запрос MongoDB, чтобы отобразить поля restaurant_id, name, район и кухня для всех документов в ресторане collection.
insertToAnswer(2, db.restaurants.find({}, { "restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1 }).toArray());

// 3.	Напишите запрос MongoDB, чтобы отобразить поля restaurant_id, name, район и кухня, но исключите поле _id для всех документов в ресторане коллекции.
insertToAnswer(3, db.restaurants.find({}, { "restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1, "_id": 0 }).toArray());

// 4.	Напишите запрос MongoDB, чтобы отобразить поля restaurant_id, name, borough и zip code, но исключите поле _id для всех документов в ресторане коллекции. 
insertToAnswer(4, db.restaurants.find({}, { "restaurant_id": 1, "name": 1, "borough": 1, "address.zipcode": 1, "_id": 0 }).toArray());

// 5.	Напишите запрос MongoDB, чтобы отобразить весь ресторан, который находится в районе Бронкс. 
insertToAnswer(5, db.restaurants.find({ "borough": "Bronx" }).toArray());

// 6.	Напишите запрос MongoDB, чтобы отобразить первые 5 ресторанов, которые находятся в районе Бронкс.
insertToAnswer(6, db.restaurants.find({ "borough": "Bronx" }).limit(5).toArray());

// 7.	Напишите запрос MongoDB, чтобы отобразить следующие 5 ресторанов после пропуска первых 5, которые находятся в районе Бронкса. 
insertToAnswer(7, db.restaurants.find({ "borough": "Bronx" }).skip(5).limit(5).toArray());

// 8.	Напишите запрос MongoDB, чтобы найти рестораны, набравшие более 90 баллов.
insertToAnswer(8, db.restaurants.find({ grades: { $elemMatch: { "score": { $gt: 90 } } } }).toArray());

// 9.	Напишите запрос MongoDB, чтобы найти рестораны, которые набрали более 80, но менее 100 баллов.  
insertToAnswer(9, db.restaurants.find({ grades: { $elemMatch: { "score": { $gt: 80, $lt: 100 } } } }).toArray());

// 10. Напишите запрос MongoDB, чтобы найти рестораны, которые находятся по широте меньше, чем -95.754168.
insertToAnswer(10, db.restaurants.find({ "address.coord": { $lt: -95.754168 } }).toArray());

// 11. Напишите запрос MongoDB, чтобы найти рестораны, которые не готовят ни одной «американской» кухни, с оценкой их баллов более 70 и широтой -65,754168.
insertToAnswer(11, db.restaurants.find({
    $and: [
        { "cuisine": { $ne: "American " } },
        { "grades.score": { $gt: 70 } },
        { "address.coord": { $lt: -65.754168 } }
    ]
}).toArray());

// 12. Напишите запрос MongoDB, чтобы найти рестораны, которые не готовят ни одной «американской» кухни и набрали более 70 баллов и находятся на долготе менее -65,754168.
// Примечание. Выполните этот запрос, не используя $ и оператор.
insertToAnswer(12, db.restaurants.find({
    "cuisine": { $ne: "American " },
    "grades.score": { $gt: 70 },
    "address.coord": { $lt: -65.754168 }
}).toArray());

// 13. Напишите запрос MongoDB, чтобы найти рестораны, которые не готовят ни одной «американской» кухни и получили оценку «А», не принадлежащую району Бруклин. Документ должен отображаться в соответствии с кухней в порядке убывания.
insertToAnswer(13, db.restaurants.find({
    "cuisine": { $ne: "American" },
    "grades.grade": "A",
    "borough": { $ne: "Brooklyn" }
}).sort({ "cuisine": -1 }).toArray());

// 14. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название, район и кухню для тех ресторанов, которые в качестве первых трех букв назвали «Wil».
insertToAnswer(14, db.restaurants.find(
    { name: /^Wil/ },
    {
        "restaurant_id": 1,
        "name": 1, "borough": 1,
        "cuisine": 1
    }
).toArray());

// 15. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название, район и кухню для тех ресторанов, которые содержат «ces» в качестве последних трех букв в названии.
insertToAnswer(15, db.restaurants.find(
    { name: /ces$/ },
    {
        "restaurant_id": 1,
        "name": 1, "borough": 1,
        "cuisine": 1
    }
).toArray());

// 16. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название, район и кухню для тех ресторанов, которые содержат «Reg» в виде трех букв где-то в своем названии.
insertToAnswer(16, db.restaurants.find(
    { "name": /.*Reg.*/ },
    {
        "restaurant_id": 1,
        "name": 1, "borough": 1,
        "cuisine": 1
    }
).toArray());

// 17. Напишите запрос MongoDB, чтобы найти рестораны, которые относятся к району Бронкс и готовят американское или китайское блюдо.
insertToAnswer(17, db.restaurants.find({
    "borough": "Bronx",
    $or: [
        { "cuisine": "American " },
        { "cuisine": "Chinese" }
    ]
}).toArray());

// 18. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название, район и кухню для тех ресторанов, которые относятся к району Статен-Айленд или Квинсу или Бронксу или Бруклину.
insertToAnswer(18, db.restaurants.find(
    { "borough": { $in: ["Staten Island", "Queens", "Bronx", "Brooklyn"] } },
    {
        "restaurant_id": 1,
        "name": 1, "borough": 1,
        "cuisine": 1
    }
).toArray());

// 19. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название, район и кухню для тех ресторанов, которые не относятся к району Стейтен-Айленд или Квинсу или Бронксу или Бруклину.
insertToAnswer(19, db.restaurants.find(
    { "borough": { $nin: ["Staten Island", "Queens", "Bronx", "Brooklyn"] } },
    {
        "restaurant_id": 1,
        "name": 1, "borough": 1,
        "cuisine": 1
    }
).toArray());

// 20. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название, район и кухню для тех ресторанов, которые набрали не более 10 баллов.
insertToAnswer(20, db.restaurants.find(
    {
        "grades.score": { $not: { $gt: 10 } }
    },
    {
        "restaurant_id": 1,
        "name": 1, "borough": 1,
        "cuisine": 1
    }
).toArray());

// 21. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название, район и кухню для тех ресторанов, в которых готовили блюда, кроме «американского» и «китайского», или название ресторана начинается с буквы «Wil».
insertToAnswer(21, db.restaurants.find({
    $or: [
        { name: /^Wil/ },
        {
            "$and": [
                { "cuisine": { $ne: "American " } },
                { "cuisine": { $ne: "Chinese" } }
            ]
        }
    ]
}, { "restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1 }).toArray());

// 22. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название и оценки для тех ресторанов, которые достигли оценки «А» и набрали 11 баллов по ISODate «2014-08-11T00:00:00Z» среди многих дат опросов.
insertToAnswer(22, db.restaurants.find({
    "grades.date": ISODate("2014-08-11T00:00:00Z"),
    "grades.grade": "A",
    "grades.score": 11
}, { "restaurant_id": 1, "name": 1, "grades": 1 }).toArray());

// 23. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название и оценки для тех ресторанов, где 2-й элемент массива оценок содержит оценку «А» и оценку 9 на ISODate «2014-08-11T00:00:00Z».
insertToAnswer(23, db.restaurants.find({
    "grades.1.date": ISODate("2014-08-11T00:00:00Z"),
    "grades.1.grade": "A",
    "grades.1.score": 9
}, { "restaurant_id": 1, "name": 1, "grades": 1 }).toArray());

// 24. Напишите запрос MongoDB, чтобы найти идентификатор ресторана, название, адрес и географическое местоположение для тех ресторанов, где 2-й элемент массива координат содержит значение, которое больше 42 и до 52.
insertToAnswer(24, db.restaurants.find({
    "address.coord.1": { $gt: 42, $lte: 52 }
}, { "restaurant_id": 1, "name": 1, "address": 1, "coord": 1 }).toArray());

// 25. Напишите запрос MongoDB, чтобы расположить названия ресторанов в порядке возрастания вместе со всеми столбцами.
insertToAnswer(25, db.restaurants.find().sort({ "name": 1 }).toArray());

// 26. Напишите запрос MongoDB, чтобы расположить названия ресторанов по убыванию вместе со всеми столбцами.
insertToAnswer(26, db.restaurants.find().sort({ "name": -1 }).toArray());

// 27. Напишите запрос MongoDB, чтобы расположить название кухни в порядке возрастания, а для этой же кухни район должен быть в порядке убывания.
insertToAnswer(27, db.restaurants.find().sort({ "cuisine": 1, "borough": -1 }).toArray());

// 28. Напишите запрос MongoDB, чтобы узнать, содержат ли все адреса улицу или нет.
insertToAnswer(28, db.restaurants.find({
    "address.street": { $exists: true }
}).toArray());

// 29. Напишите запрос MongoDB, который выберет все документы в коллекции ресторанов, где значение поля координат равно Double.
insertToAnswer(29, db.restaurants.find({
    "address.coord": { $type: 1 }
}).toArray());

// 30. Напишите запрос MongoDB, который выберет идентификатор ресторана, название и оценки для тех ресторанов, который возвращает 0 в качестве остатка после деления счета на 7.
insertToAnswer(30, db.restaurants.find({
    "grades.score": { $mod: [7, 0] }
}, { "restaurant_id": 1, "name": 1, "grades": 1 }).toArray());

// 31. Напишите запрос MongoDB, чтобы найти название ресторана, район, долготу и отношение, а также кухню для тех ресторанов, в которой где-то в названии есть три буквы «mon».
insertToAnswer(31, db.restaurants.find({
    name: { $regex: "mon.*", $options: "i" }
}, {
    "name": 1,
    "borough": 1,
    "address.coord": 1,
    "cuisine": 1
}).toArray());

// 32. Напишите запрос MongoDB, чтобы найти название ресторана, район, долготу и широту и кухню для тех ресторанов, в которых первые три буквы названия - «Безумный».
insertToAnswer(32, db.restaurants.find({
    name: { $regex: /^Mad/i }
}, {
    "name": 1,
    "borough": 1,
    "address.coord": 1,
    "cuisine": 1
}).toArray());


execSync(`mongoexport --db ${dbName} --collection answers --jsonArray --out answers.json`);

conn.close()