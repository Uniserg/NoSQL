var conn = new Mongo();
var db = conn.getDB("University");

db.dropDatabase();

const insertToAnswer = function (id, ans) {
    db.Answers.updateOne(
        { _id: id },
        { $set: { answer: ans } },
        { upsert: true }
    );
    return ans;
};

function loadStudentsData() {
    // Загружаем данные из JSON-файла в объект
    var studentsJson = require("./Students.json");
  
    // Назначение _id и вставка данных в коллекцию Students
    studentsJson.forEach(function (student) {
      try {
        student['_id'] = student['Номер'];
      } catch (error) {
        print(`Ошибка назначения _id при ${tojson(student)}`);
      }
    });
  
    db.Students.insertMany(studentsJson);
  }

// Использование функции для загрузки данных
loadStudentsData();

// 1. Импортируйте Students.json в локальный MongoDB, DB University, Collection Students. Ключ
// задайте по полю Номер.

// mongoimport --db University --collection Students --file Students.json --jsonArray --drop

// db.StudentsTemp.insertMany(
//     db.Students.find().map(function (doc) {
//         doc._id = doc['Номер'];
//         return doc;
//     }).toArray());
// db.StudentsTemp.renameCollection("Students", true);
// db.StudentsTemp.drop();

// 2. Выведите статистику по DB. Какое общее количество объектов?
insertToAnswer(2, db.stats().objects.toNumber());

// 3. Какое общее количество документов в коллекции Students?
insertToAnswer(3, db.Students.countDocuments({}));


// 4. Сколько студентов обучается на специальности Математика?
insertToAnswer(4, db.Students.countDocuments({ "Специальность": "Математика" }));

//     5. Сколько студентов обучается на специальности Математика на втором курсе?
insertToAnswer(5, db.Students.countDocuments({ "Специальность": "Математика", "Курс": 2 }));

//     6. Сколько студентов обучается на специальности Математика кроме второго курса?
insertToAnswer(6, db.Students.countDocuments({ "Специальность": "Математика", "Курс": { $ne: 2 } }));

//     7. Сколько студентов обучается на специальности Математика старше третьего курса?
insertToAnswer(7, db.Students.countDocuments({ "Специальность": "Математика", "Курс": { $gt: 3 } }));

//     8. Сколько студентов обучается на специальности Юриспруденция и Экономика кроме третьего
// курса?
insertToAnswer(8, db.Students.countDocuments({ $or: [{ "Специальность": "Юриспруденция" }, { "Специальность": "Экономика" }], "Курс": { $ne: 3 } }));

//     9. Граждане скольких стран обучаются в ВУЗе?
insertToAnswer(9, db.Students.distinct("Гражданство").length);

//     10. Выведите в файл 10 первых документов из коллекции Students только с полями _id, Факультет,
//     Гражданство. Отсортируйте по полю Гражданство.
insertToAnswer(10, db.Students.find({}, { "_id": 1, "Факультет": 1, "Гражданство": 1 }).sort({ "Гражданство": 1 }).limit(10).toArray())

// 11. Импортируйте Languages2.json в локальный MongoDB, DB University, Collection Languages2.
// mongoimport --db University --collection Languages2 --file Languages2.json --jsonArray --drop
// Загружаем данные из JSON-файла в коллекцию Languages2
var languagesData = require("./Languages2.json");
db.Languages2.insertMany(languagesData);

// Создание объекта view для коллекций Students и Languages2 с JOIN-ом
db.createView("StudentsLanguages", "Students", [
    {
        $lookup: {
            from: "Languages2",
            localField: "idLanguage",
            foreignField: "_id",
            as: "languageInfo"
        }
    },
    {
        $unwind: "$languageInfo"
    },
    {
        "$addFields": { "Языки": "$languageInfo.Languages" }
    },
    {
        $project: {
            "languageInfo": 0,
            "idLanguage": 0
        }
    }
]);

// 12. Какие языки изучает студент 1032102469?
insertToAnswer(12, db.StudentsLanguages.findOne({ "Номер": "1032102469" }, { "Языки": 1, "_id": 0 })["Языки"]);

//     13. Сколько студентов изучают какие-либо языки?
insertToAnswer(13, db.Students.find({ "idLanguage": { $exists: true, $ne: [] } }).count());

//     14. Сколько студентов не изучают никаких языков?
insertToAnswer(14, db.Students.find({ "idLanguage": { $exists: false } }).count());

//     15. Сколько студентов изучают немецкий язык?
insertToAnswer(15, db.StudentsLanguages.find({ "Языки": "немецкий" }).count());

//     16. Какой _id у документа в коллекции Languages2, содержащего массив из трех языков?
insertToAnswer(16, db.Languages2.findOne({ "Languages": { $size: 3 } })["_id"]);

//     17. Сколько студентов изучают три языка?
insertToAnswer(17, db.StudentsLanguages.find({ "Языки": { $size: 3 } }).count());

//     18. Сколько студентов изучают пять языков?
insertToAnswer(18, db.Languages2.find({ "Languages": { $size: 5 } }).count());

//     19. Создайте индексы в коллекции Students для нескольких полей и один составной индекс.
db.Students.createIndex({ "Факультет": 1 })
db.Students.createIndex({ "Специальность": 1 })
db.Students.createIndex({ "Курс": 1 })
db.Students.createIndex({ "Факультет": 1, "Специальность": 1, "Курс": 1 })

// 20. Увеличьте всем студентам курс на 1. Максимальный курс может быть 6. Проверьте все данные и
// при необходимости исправьте. Выведите в файл с 500 по 2000 записи.
db.Students.updateMany({ "Курс": { $lt: 6 } }, { $inc: { "Курс": 1 } });
insertToAnswer(20, db.Students.find().skip(499).limit(2000).toArray());

// 21. В номере студенческого билета 4-6 регистры отвечают за год поступления. Если 4 регистр равен 2,
//     то студент поступил после 2000 года, если 0 – то до 2000 года. Сколько студентов поступило в
// 2009 году?
insertToAnswer(21, db.Students.countDocuments({ "Номер": { $regex: /^.{3}209.*$/ } }))

//     22. Сколько студентов поступило с 2000 по 2005 годы?
insertToAnswer(22, db.Students.countDocuments({ "Номер": { $regex: /^.{3}20[0-5].*$/ } }))

//     23. Замените в файле Students.json несколько значений Курс с числового на текстовый. Загрузите
// файл Students.json в новую коллекцию (например Students2). Вычислите, какое количество
// значений Курс числовое, а какое текстовое.

// Клонирование коллекции
db.Students.aggregate([
    { $match: {} },
    { $out: "Students2" }
]);
// У первых 5 записей меняем тип на строковый
db.Students2.find().limit(5).forEach(function (doc) {
    db.Students2.updateOne(
        { "_id": doc._id },
        { $set: { "Курс": doc['Курс'].toString() } }
    );
});

insertToAnswer(23, db.Students2.aggregate([
    {
        $group: {
            _id: { $type: "$Курс" },
            count: { $sum: 1 }
        }
    },
    {
        $project: {
            type: "$_id",
            count: "$count",
            _id: 0
        }
    }
]).toArray());
// 24. * Добавьте в коллекцию Students каждому студенту соответствующий массив Languages.
//     Выведите в файл с 100 по 200 записи
insertToAnswer(24, db.StudentsLanguages.find().skip(99).limit(200).toArray())


// Экспорт в json
// var exportPath = "./answers-from-js.json";
// var exportCommand = `mongoexport --db University --collection Answers --out ${exportPath} --jsonArray`;

var exportPath = "./answers-js.json";
// var exportCommand = `mongoexport --db University --collection Answers --out ${exportPath} --jsonArray`;

try {
    // Записываем результат экспорта коллекции Answers в файл
    var result = db.Answers.find().toArray();
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