"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.log = (...things) => {
    console.log(new Date().toISOString(), ...things);
};
exports.error = error => {
    console.error(error);
    process.exit(1);
};
