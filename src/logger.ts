export const log = (...things) => {
  // tslint:disable-next-line no-console
  console.log(new Date().toISOString(), ...things);
};

export const error = error => {
  // tslint:disable-next-line no-console
  console.error(error);
  process.exit(1);
};
