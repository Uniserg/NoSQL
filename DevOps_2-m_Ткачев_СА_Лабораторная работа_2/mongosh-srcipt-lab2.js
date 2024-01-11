var conn = new Mongo();
var db = conn.getDB("University");


db.AnswersLab2.drop()

const insertToAnswer = function (id, ans) {
    db.AnswersLab2.updateOne(
        { _id: id },
        { $set: { answer: ans } },
        { upsert: true }
    );
    return ans;
};


// Булевы операторы
// 1.1. Сколько студентов в данных момент обучается по специальностям, кроме Юриспруденция и Экономика?
insertToAnswer(1.1, db.Students.countDocuments({ "Специальность": { $nin: ["Юриспруденция", "Экономика"] } }))

// 1.2. Сколько всего студентов в данных момент обучается по специальности Математика на первом курсе и Экономика, кроме третьего курса?
insertToAnswer(1.2, db.Students.countDocuments({ $or: [{ "Специальность": "Математика", "Курс": 1 }, { "Специальность": "Экономика", "Курс": { $ne: 3 } }] }))

// 1.3. Сколько всего граждан РФ в данных момент обучается по специальности Юриспруденция и сколько иностранных граждан не изучают французский язык?
insertToAnswer(1.3, db.Students.countDocuments({ $or: [{ "Гражданство": "РФ", "Специальность": "Юриспруденция" }, { "Гражданство": { $ne: "РФ" }, "Языки": { $ne: "французский" } }] }))


// 2. Агрегация (aggregate)
// 2.1. Выведите количество студентов по формам обучения
insertToAnswer(2.1, db.Students.aggregate([
    { $group: { _id: "$Форма обучения", count: { $sum: 1 } } }
])
    .toArray()
    .reduce((acc, doc) => Object.assign(acc, { [doc._id]: doc.count }), {}));

// 2.2. Выведите количество выпускников по факультетам
insertToAnswer(2.2, db.Students.aggregate([
    { $match: { Статус: "Завершение" } },
    { $group: { _id: "$Факультет", count: { $sum: 1 } } }
])
    .toArray()
    .reduce((acc, doc) => Object.assign(acc, { [doc._id]: doc.count }), {}),);

// 2.3. Выведите количество обучающихся на 2 курсе Юридического факультета по специальностям.
insertToAnswer(2.3, db.Students.aggregate([
    { $match: { Факультет: "Юридический", Курс: 2 } },
    { $group: { _id: "$Специальность", count: { $sum: 1 } } }
])
    .toArray()
    .reduce((acc, doc) => Object.assign(acc, { [doc._id]: doc.count }), {}));

// 2.4. Выведите среднее значение поля Курс для отчисленных студентов Экономического факультета.
insertToAnswer(2.4, db.Students.aggregate([
    { $match: { Факультет: "Экономический", Статус: "Отчисление" } },
    { $group: { _id: null, avgKurs: { $avg: "$Курс" } } },
]).toArray()[0]['avgKurs'])

// 2.5. Выведите минимальное значение поля Курс для отчисленных студентов по факультетам.

insertToAnswer(2.5, db.Students.aggregate([
    { $match: { Статус: "Отчисление" } },
    { $group: { _id: "$Факультет", minKurs: { $min: "$Курс" } } }])
    .toArray()
    .reduce((acc, doc) => Object.assign(acc, { [doc._id]: doc.minKurs }), {}));

// insertToAnswer(2.5, db.Students.aggregate([
//     { $match: { Статус: "Отчисление" } },
//     { $group: { _id: "$Факультет", minKurs: { $min: "$Курс" } } }
// ]).map(doc => ({ [doc._id]: doc.minKurs })).toArray());

// db.Students.aggregate([
//     { $match: { Статус: "Отчисление" } },
//     { $group: { _id: "$Факультет", minKurs: { $min: "$Курс" } } },
//     {}
//     // { $replaceWith: { $arrayToObject: "$data" }, },
// ]);

// 3. Группировка (group)
// Метод группировки устаревший. Вместо него рекомендуется использовать агрегацию с pipeline $group
//DEPRECATED SINCE VERSION 3.4
// Mongodb 3.4 deprecates the db.collection.group() method. Use db.collection.aggregate() with the $group stage or db.collection.mapReduce() instead.
// https://www.mongodb.com/docs/v4.0/reference/method/db.collection.group/
db.AnswersLab2.find({ _id: { $in: [2.1, 2.2, 2.3, 2.4, 2.5] } }).forEach(function (doc) {
    var newId = doc._id + 1;
    db.AnswersLab2.insertOne({ _id: newId, answer: doc.answer });
});


var exportPath = "./answers.json";

try {
    // Записываем результат экспорта коллекции Answers в файл
    var result = db.AnswersLab2.find().toArray();
    var jsonData = JSON.stringify(result, null, 2);

    // Записываем в файл
    var fs = require('fs');
    fs.writeFileSync(exportPath, jsonData);

    print(`Экспорт коллекции Answers завершен: ${exportPath}`);
} catch (error) {
    print(`Ошибка выполнения команды: ${error}`);
}

// Закрываем соединение с MongoDB
conn.close();

print("Сприпт успешно отработал")
