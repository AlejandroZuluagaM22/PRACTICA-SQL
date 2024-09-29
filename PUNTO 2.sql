-- Eliminar las tablas dependientes primero
DROP TABLE IF EXISTS bootcamp_alumno;
DROP TABLE IF EXISTS bootcamp_modulo;

-- Eliminar las tablas principales
DROP TABLE IF EXISTS modulo;
DROP TABLE IF EXISTS bootcamp;
DROP TABLE IF EXISTS profesor;
DROP TABLE IF EXISTS alumno;

-- Table: alumno
CREATE TABLE alumno (
    alumno_id SERIAL PRIMARY KEY,
    nombre VARCHAR(50),
    apellido VARCHAR(50),
    correo VARCHAR(50),
    telefono VARCHAR(50)
);

ALTER TABLE alumno
ADD CONSTRAINT unique_email UNIQUE (email);

ALTER TABLE customer
ALTER COLUMN email SET NOT NULL;

-- Table: bootcamp
CREATE TABLE bootcamp (
    bootcamp_id SERIAL PRIMARY KEY,
    nombre_bootcamp CHAR(100),
    fecha_inicio DATE,
    fecha_terminacion DATE
);

CREATE INDEX idx_fecha_inicio ON bootcamp (fecha_inicio);

-- Table: Profesor
CREATE TABLE profesor (
    profesor_id SERIAL PRIMARY KEY,
    nombre CHAR(50),
    apellido CHAR(50),
    correo CHAR(50),
    telefono CHAR(50),
    cargo CHAR(50)
);

ALTER TABLE profesor
ADD CONSTRAINT unique_email UNIQUE (email);

ALTER TABLE profesor
ALTER COLUMN email SET NOT NULL;


-- Table: Modulo
CREATE TABLE modulo (
    modulo_id SERIAL PRIMARY KEY,
    nombre_modulo CHAR(50),
    numero_clases INT,
    fecha_inicio DATE,
    fecha_terminacion DATE,
    profesor_id INT,
    CONSTRAINT fk_profesor FOREIGN KEY (profesor_id) REFERENCES profesor(profesor_id)
);

CREATE INDEX idx_fecha_inicio ON modulo (fecha_inicio);

-- Table: bootcamp_alumno
CREATE TABLE bootcamp_alumno (
    btc_alumn_id SERIAL PRIMARY KEY,
    bootcamp_id INT,
    alumno_id INT,
    CONSTRAINT fk_bootcamp FOREIGN KEY (bootcamp_id) REFERENCES bootcamp(bootcamp_id),
    CONSTRAINT fk_alumno FOREIGN KEY (alumno_id) REFERENCES alumno(alumno_id)
    UNIQUE (bootcamp_id, alumno_id)
);

-- Table: bootcamp_modulo
CREATE TABLE bootcamp_modulo (
    btc_mod_id SERIAL PRIMARY KEY,
    bootcamp_id INT,
    modulo_id INT,
    CONSTRAINT fk_bootcamp_modulo FOREIGN KEY (bootcamp_id) REFERENCES bootcamp(bootcamp_id),
    CONSTRAINT fk_modulo FOREIGN KEY (modulo_id) REFERENCES modulo(modulo_id)
    UNIQUE (bootcamp_id, modulo_id)
);
