import axios from "axios";

export async function uploadToPinata(file: File): Promise<string> {
  const formData = new FormData();
  formData.append("file", file);

  const metadata = JSON.stringify({
    name: file.name,
  });
  formData.append("pinataMetadata", metadata);

  const options = JSON.stringify({
    cidVersion: 0,
  });
  formData.append("pinataOptions", options);

  try {
    const res = await axios.post(
      "https://api.pinata.cloud/pinning/pinFileToIPFS",
      formData,
      {
        headers: {
          Authorization: `Bearer ${process.env.PINATA_JWT}`,
          // Allow axios to set content-type with boundary
        },
      }
    );
    return `ipfs://${res.data.IpfsHash}`;
  } catch (error) {
    console.error("Pinata upload error:", error);
    throw new Error("Failed to upload image to IPFS");
  }
}

export async function uploadToPinataJSON(body: any): Promise<string> {
  const url = `https://api.pinata.cloud/pinning/pinJSONToIPFS`;

  try {
    const res = await axios.post(url, body, {
      headers: {
        Authorization: `Bearer ${process.env.PINATA_JWT}`,
        'Content-Type': 'application/json'
      }
    });

    return `ipfs://${res.data.IpfsHash}`;
  } catch (error) {
     console.error("Pinata JSON upload error:", error);
     throw new Error("Failed to upload metadata to IPFS");
  }
}
