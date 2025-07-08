import {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  CompletedPart,
} from "@aws-sdk/client-s3";
import * as fs from "fs";
import * as path from "path";
import * as dotenv from "dotenv";

dotenv.config(); // Carga variables .env en process.env

const REGION = process.env.AWS_REGION || "us-east-1";
const BUCKET_NAME = process.env.BUCKET_NAME || "tu-bucket";
const BASE_PATH = path.resolve(__dirname, "D://Temp");
const PART_SIZE = 5 * 1024 * 1024;

async function multipartUpload(fileName: string) {
  const filePath = path.join(BASE_PATH, fileName);
  const keyName = fileName;

  if (!fs.existsSync(filePath)) {
    console.error(`El archivo "${filePath}" no existe.`);
    process.exit(1);
  }

  // El SDK detecta automáticamente las credenciales en process.env
  const s3Client = new S3Client({
    region: REGION,
  });

  try {
    const createMultipartUploadCommand = new CreateMultipartUploadCommand({
      Bucket: BUCKET_NAME,
      Key: keyName,
    });

    const createMultipartUploadResponse = await s3Client.send(createMultipartUploadCommand);
    const uploadId = createMultipartUploadResponse.UploadId;

    if (!uploadId) {
      throw new Error("No se pudo obtener el UploadId para la carga multiparte.");
    }

    console.log(`Carga multiparte iniciada. UploadId: ${uploadId}`);

    const fileSize = fs.statSync(filePath).size;
    let partNumber = 1;
    let start = 0;
    const uploadedParts: CompletedPart[] = [];

    while (start < fileSize) {
      const end = Math.min(start + PART_SIZE, fileSize);
      const partLength = end - start;

      const partStream = fs.createReadStream(filePath, {
        start,
        end: end - 1,
      });

      console.log(`Subiendo parte ${partNumber}, bytes ${start} a ${end - 1}`);

      const uploadPartCommand = new UploadPartCommand({
        Bucket: BUCKET_NAME,
        Key: keyName,
        UploadId: uploadId,
        PartNumber: partNumber,
        Body: partStream,
        ContentLength: partLength,
      });

      const uploadPartResponse = await s3Client.send(uploadPartCommand);

      if (!uploadPartResponse.ETag) {
        throw new Error(`No se recibió ETag para la parte ${partNumber}`);
      }

      uploadedParts.push({
        ETag: uploadPartResponse.ETag,
        PartNumber: partNumber,
      });

      console.log(`Parte ${partNumber} subida correctamente. ETag: ${uploadPartResponse.ETag}`);

      partNumber++;
      start += PART_SIZE;
    }

    const completeMultipartUploadCommand = new CompleteMultipartUploadCommand({
      Bucket: BUCKET_NAME,
      Key: keyName,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: uploadedParts,
      },
    });

    const completeResponse = await s3Client.send(completeMultipartUploadCommand);

    console.log("Carga multiparte completada exitosamente.");
    console.log(`Ubicación del objeto: ${completeResponse.Location}`);
  } catch (error) {
    console.error("Error durante la carga multiparte:", error);

    if (error instanceof Error && (error as any).uploadId) {
      const uploadId = (error as any).uploadId;
      const abortMultipartUploadCommand = new AbortMultipartUploadCommand({
        Bucket: BUCKET_NAME,
        Key: fileName,
        UploadId: uploadId,
      });
      await s3Client.send(abortMultipartUploadCommand);
      console.log("Carga multiparte abortada.");
    }
  }
}

const args = process.argv.slice(2);
if (args.length !== 1) {
  console.error("Uso: node upload.js <nombre-archivo>");
  process.exit(1);
}

const fileName = args[0];

multipartUpload(fileName).catch((error) => {
  console.error("Error inesperado:", error);
});
