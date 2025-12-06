const fs = require("node:fs");
const readStream = fs.createReadStream("big.txt", { encoding: "utf-8" });

const readStream2 = fs.createReadStream("source.txt", { encoding: "utf-8" });
const writeStream1 = fs.createWriteStream("dest.txt", { encoding: "utf-8" });

const { createGzip } = require("node:zlib");
const zip = createGzip();
const readStream3 = fs.createReadStream("data.txt");
const writeStream2 = fs.createWriteStream("data.txt.gz");

// Q1
readStream.on("readable", () => {
  console.log(readStream.read());
});

// Q2
readStream2.on("data", (chunk) => {
  console.log(chunk);
  writeStream1.write(chunk);
});
readStream2.on("end", () => {
  writeStream1.end();
  console.log("ended");
});

// Q3
readStream3.pipe(zip).pipe(writeStream2);

// Part2 Q1
const http = require("node:http");
const path = require("path");

const filePath = path.join(__dirname, "users.json");

function readUsers() {
  try {
    const data = fs.readFileSync(filePath, "utf-8");
    return data.trim() ? JSON.parse(data) : [];
  } catch {
    return [];
  }
}

function saveUsers(users) {
  fs.writeFileSync(filePath, JSON.stringify(users, null, 2));
}

const server = http.createServer((req, res) => {
  const { method, url } = req;

  if (url === "/user" && method === "POST") {
    let body = "";

    req.on("data", (chunk) => (body += chunk));

    req.on("end", () => {
      const newUser = JSON.parse(body);
      let users = readUsers();

      if (users.some((u) => u.email === newUser.email)) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ message: "Email already exists" }));
      }

      newUser.id = users.length + 1;
      users.push(newUser);
      saveUsers(users);

      res.writeHead(201, { "content-type": "application/json" });
      res.end(JSON.stringify({ message: "User Added", user: newUser }));
    });
  } else if (url.startsWith("/user/") && method === "PATCH") {
    const id = Number(url.split("/")[2]);
    let users = readUsers();
    let index = users.findIndex((u) => u.id === id);

    if (index === -1) {
      res.writeHead(404, { "content-type": "application/json" });
      return res.end(JSON.stringify({ message: "User Not Found" }));
    }

    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      let updatedData = JSON.parse(body);

      users[index] = { ...users[index], ...updatedData };

      if (
        updatedData.email &&
        users.some((u) => u.email === updatedData.email && u.id !== id)
      ) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ message: "Email already exists" }));
      }

      saveUsers(users);

      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ message: "User Updated", user: users[index] }));
    });
  } else if (url.startsWith("/user/") && method === "DELETE") {
    const id = Number(url.split("/")[2]);
    let users = readUsers();

    const exists = users.some((u) => u.id === id);
    if (!exists) {
      res.writeHead(404, { "content-type": "application/json" });
      return res.end(JSON.stringify({ message: "User Not Found" }));
    }

    users = users.filter((u) => u.id !== id);
    saveUsers(users);

    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ message: "User Deleted" }));
  } else if (url === "/user" && method === "GET") {
    let users = readUsers();
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(users));
  } else if (url.startsWith("/user/") && method === "GET") {
    const id = Number(url.split("/")[2]);
    let users = readUsers();
    let user = users.find((u) => u.id === id);

    if (!user) {
      res.writeHead(404, { "content-type": "application/json" });
      return res.end(JSON.stringify({ message: "User Not Found" }));
    }

    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(user));
  } else {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ message: "Page Not Found" }));
  }
});

server.listen(3000, () => console.log("Server running on port 3000..."));

// Part 3
// Q1
// الـ Event Loop في Node.js هو حلقة بتدير تنفيذ الـ callbacks والـ I/O بطريقة non-blocking: الكود بيتنفّذ في call stack، بينما I/O وعمليات بطيئة بتندفع لِـ libuv/thread pool، وبعد ما تخلص بتتسجل نتائجها في طوابير، والـ event loop بينفّذ الطوابير دي مرحلة بمرحلة (timers → pending I/O → poll → check → close)، مع أولوية خاصة

// Q2
// libuv هي مكتبة C منخفضة المستوى،
// وهي اللي بتدي Node.js القدرة يعمل I/O غير متزامن (Asynchronous Non-Blocking) عبر:

// تشغيل الـ Event Loop

// تشغيل Thread Pool للعمليات الثقيلة

// التعامل مع الشبكات والملفات

// تنفيذ timers بشكل حقيقي

// توفير Layer موحّد يعمل على كل الأنظمة

// Q3
// Node.js بيتعامل مع الـ Async operations عن طريق:
// JavaScript ينفّذ الكود السريع فقط.
// libuv يستلم أي شغل تقيل أو I/O.
// Thread Pool ينفّذ العمليات اللي بتاخد وقت (ملفات، DNS، crypto).
// OS Kernel ينفّذ Network events.
// Event Loop يرجّع النتائج لكودك حسب المرحلة المناسبة.
// Microtasks ليهم أولوية أعلى (Promises & nextTick).

// Q4
// Call Stack
// المكان اللي كودك الحالي بيتنفذ فيه (Synchronous).
// Event Queue
// طابور فيه Callbacks جاهزين، مستنيين بس الستاك يفضى.
// Event Loop
// الوسيط اللي ينقل الكولباكس من الـ Queue للـ Stack لما يبقى فاضي.

// Q5
//  Thread Pool
//  4 threads في libuv،
//  (filesystem، crypto، DNS، zlib…).
//  ليه؟
// لحماية الـ main thread من الـ blocking،
// وللسماح لـ Node.js يشتغل Async فعليًا.

// Q6
// Node.js يشغّل Non-Blocking I/O عبر Libuv و Thread Pool بدون ما يوقف الـ Event Loop.
// الكود اللي يحتاج CPU لو كان Sync → يوقف السيرفر. لو Async → بيروح للـ Thread Pool.
// Blocking = بيوقف كل حاجة
// Non-Blocking = يشغّل حاجات كتير في نفس الوقت
// Event Loop = اللي ينفذ callbacks لما تكون جاهزة
// Thread Pool = يشيل عنه شغل التقيل
