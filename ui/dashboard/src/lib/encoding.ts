import type {KeyDetail} from "../types/api";
export function convertEncoding(value:string,from:KeyDetail["encoding"],to:KeyDetail["encoding"]):string {
 let bytes:Uint8Array;
 if(from==="utf8"){bytes=new TextEncoder().encode(value);}
 else if(from==="base64"){bytes=Uint8Array.from(atob(value),c=>c.charCodeAt(0));}
 else {if(!/^(?:[a-fA-F0-9]{2})*$/.test(value)){throw new Error("Invalid hexadecimal value.");}bytes=Uint8Array.from(value.match(/../g)??[],n=>parseInt(n,16));}
 if(to==="utf8"){try{return new TextDecoder("utf-8",{fatal:true}).decode(bytes);}catch{throw new Error("This value is binary; use Base64 or hex.");}}
 if(to==="hex"){return Array.from(bytes,n=>n.toString(16).padStart(2,"0")).join("");}
 let result="";for(let i=0;i<bytes.length;i+=8192){result+=String.fromCharCode(...bytes.subarray(i,i+8192));}return btoa(result);
}
