const fs = require('fs');

var conn = new Mongo();

db = conn.getDB("kladr");

db.dropDatabase();
conn.getDB("kladr_1").dropDatabase();
conn.getDB("kladr_2").dropDatabase();

const insertToAnswer = function (id, ans) {
    print(`Задание ${id} выполнено. Запись в answer...`);
    db.Answers.updateOne(
        { _id: id },
        { $set: { answer: ans } },
        { upsert: true }
    );
    return ans;
};

// 1. Импортируйте КЛАДР в локальный MongoDB.
function importKladrsData() {
    const dataPath = "./data";
    const files = fs.readdirSync(dataPath);

    print("Импорт данных...");

    files.forEach(function (file) {
        let jsonData = fs.readFileSync(`${dataPath}/${file}`, 'utf-8');
        jsonData = jsonData.replace(/^\uFEFF/, '');
        jsonData = jsonData.replace(/_id/g, '\"_id\"').replace(/details/g, '\"details\"');
        db.kladr.insertMany(JSON.parse(jsonData));
    });
}

importKladrsData();


// 2. Сколько уровней адресов в КЛАДРе?
insertToAnswer(2, db.kladr.distinct('details.level').length);

// 3. Перечислите наименование уровней.
insertToAnswer(3, db.kladr.distinct('details.level'));

// 4. Сколько объектов в каждом уровне?
insertToAnswer(4, db.kladr.aggregate([
    { $group: { _id: '$details.level', count: { $sum: 1 } } }
]).toArray());

// 5. Составьте перечень типов населенных пунктов.
insertToAnswer(5, db.kladr.distinct('details.type'));

// 6. Сколько населенных пунктов начинается на букву М?
insertToAnswer(6, db.kladr.countDocuments({ 'details.name': /^М/ }));

// 7. Сколько населенных пунктов начинается на каждую букву?
insertToAnswer(7, db.kladr.aggregate([
    {
        $group: {
            _id: { $substrCP: ['$details.name', 0, 1] },
            count: { $sum: 1 }
        }
    },
    { $sort: { _id: 1 } }
]).toArray());

// 8. Какое назначение ключа district?
insertToAnswer(8, db.kladr.distinct('details.district'));

// 9. Какое количество объектов у каждого значения district?
insertToAnswer(9, db.kladr.aggregate([
    {
        $group: {
            _id: '$details.district',
            count: { $sum: 1 }
        }
    }
]).toArray());

// 10. Добавление массива
// 10.1. Создайте курсор: первые 100 документов населенных пунктов 4 уровня. Ключи документов: _id, type, name.
const getCursor = () => db.kladr.find({ 'details.level': 4 }, { _id: 1, 'details.type': 1, 'details.name': 1 }).limit(100);

// 10.2. Добавьте курсор в новую коллекцию. Выведите результат добавления.
// const newCollectionName = 'newCollection';
// db[cursor].forEach(doc => db[newCollectionName].insertOne(doc));

// 10.2. Добавьте курсор в новую коллекцию. Выведите результат добавления.
getCursor().forEach(function (doc) {
    db.newCollection.insertOne(doc);
});
insertToAnswer(10.2, db.newCollection.find({}).toArray());

// 10.3. Преобразуйте курсор в массив.
const arrayData = getCursor().toArray();
print("Задание 10.3 - выполнено");

// 10.4. Добавьте массив в коллекцию. Выведите результат добавления.
db.newCollection2.insertMany(arrayData);
insertToAnswer(10.4, db.newCollection.find({}).toArray());
print("Задание 10.4 - выполнено");

// 10.5. Вставьте в новую коллекцию с 200 по 300 документов населенных пунктов 4 уровня,
// входящих в Центральный федеральный округ.
const centralDistrictDocs = db.kladr.find({ 'details.level': 4, 'details.district': 'Центральный' }).skip(199).limit(100);
centralDistrictDocs.forEach(function (doc) {
    db.newCollection.updateOne({ _id: doc._id }, { $set: doc }, { upsert: true });
});
print("Задание 10.5 - выполнено");

// 11. Создайте индекс по ключу wikiname.
db.kladr.createIndex({ 'details.region.wikiname': 1 });
print("Задание 11 - выполнено");

// 12. Создайте уникальный индекс по ключу name населенного пункта.

// function groupBy(collection, keys) {
//     const groupFields = {};
//     keys.forEach(key => {
//         groupFields[key.replace(".", "_")] = `$${key}`;
//     });

//     return collection.aggregate([
//         {
//             "$group": {
//                 "_id": groupFields,
//                 "dups": { "$push": "$$ROOT" },
//                 "count": { "$sum": 1 }
//             }
//         },
//         { "$match": { "count": { "$gt": 1 } } }
//     ]);
// }


// Получить имена полей из первого документа в коллекции
const sampleDocument = db.kladr.findOne();
const fieldNames = Object.keys(sampleDocument).filter(field => field !== "_id");

// Сформировать объект для группировки
const groupFields = {};
fieldNames.forEach(fieldName => {
    groupFields[fieldName] = `$${fieldName}`;
});

// Найти дубликаты
// const getDuplicateDocs = () => db.kladr.aggregate([
//     {
//         "$group": {
//             "_id": groupFields,
//             "count": { "$sum": 1 },
//             "dups": { "$push": "$_id" }
//         }
//     },
//     {
//         "$match": {
//             "count": { "$gt": 1 }
//         }
//     }
// ]);

// print(`Дубликаты без учета _id:\n${getDuplicateDocs().toArray()}`);

// // Удаление дубликатов
// getDuplicateDocs().forEach(doc => {
//     doc.dups.shift(); // Оставить только один _id (оставить первый)
//     db.kladr.deleteMany({ "_id": { "$in": doc.dups } });
// });
insertToAnswer(12, "Создать индекс на уникальность нельзя, так как name - неуникальное поле. Удалить нельзя, так как разные документы.")
print("Задание 12 - выполнено. Создать индекс на уникальность нельзя, так как name - неуникальное поле. Удалить нельзя, так как разные документы.");

// // 13. Создайте уникальный составной индекс по ключам name и code населенного пункта.
db.kladr.createIndex({ 'details.name': 1, 'details.code': 1 }, { unique: true });
print("Задание 13 - выполнено");

// // 14. Продемонстрируйте на любом курсоре работу функции showRecordId()
insertToAnswer(14, db.kladr.find().limit(1).showRecordId().toArray());

const { execSync } = require('child_process');


// 15. Создайте резервную копию своей рабочей базы данных
const backupCommand = 'mongodump --db kladr --out ./';
execSync(backupCommand);
console.log(`Задание 15 - Резервная копия успешно создана`);


// 16. Восстановите в новую базу данных резервную копию вашей рабочей базы данных
const restoreDatabaseCommand = 'mongorestore --db kladr_1 ./kladr';
execSync(restoreDatabaseCommand);
console.log(`Задание 16 - База данных успешно восстановлена`);


// 17. Восстановите в новую базу данных резервную копию одной коллекции вашей рабочей базы данных
const restoreCollectionCommand = 'mongorestore --db kladr_2 --collection kladr --drop ./kladr/kladr.bson';
execSync(restoreCollectionCommand);
console.log('Задание 17 - Коллекция успешно восстановлена');