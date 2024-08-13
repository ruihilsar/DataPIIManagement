package algorithm.stringAlgorithm;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.AlgorithmParameters;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import algorithm.Algorithm;

public class AESWithSaltPadding implements Algorithm{
		
		/**
	     * originally from Queens project.
	     */
	    private static final String PASSWORD = "Qur!Arc!MRO!06012016"; 
	 
	    /**
	     * originally from Queens project PHP code. 16 bytes
	     */
	    private static final String SALT = "F1EB9845886E4512AEC270D41FA1B1C4"; 

	    /**
	     * set character set to UTF-8.
	     */
	    private static final String CHARATERSET = "UTF-8";
	    
	    /**
	     * iteration counts.
	     */
	    private static final int PSWDITERATIONS = 65536;

	    /**
	     * key length.
	     */
	    private static final int KEY_SIZE = 128; 
	    
	    /**
	     * algorithm AES.
	     */
	    private static final String ALGORITHM = "AES";
	    
	    /**
	     * padding.
	     */
	    private static final String PADDING = "AES/CBC/PKCS5Padding"; 
	    
	    /**
	     * SecretKeyFactory Algorithms.
	     */
	    private static final String SECRET_KEY_FAC_ALG = "PBKDF2WithHmacSHA1";
	    
	    /**
	     * iv byte array.
	     */
	    private static byte[] ivBytes;
	    
	    public AESWithSaltPadding() {
			
		}
		
		public String getName() {
			return "AESWithSaltPadding";
		}
	    
		@Override
		public <T> T init(T dataValue) {
			String originalStr = (String) dataValue;
			String result = encrypt(originalStr);
		    
		    return (T) result;
		}
		
	    /**
	     * new encrypt method with salt and secret key algorithm.
	     *
	     * @param input the plain text.
	     * @return encryption text.
	     */
	    public static String encrypt(final String input) {
	        
	        byte[] encryptedTextBytes = null;
	        final byte[] saltByte = SALT.getBytes(Charset.forName(CHARATERSET));
	        
	        try {
	            // Derive the key
	            final SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_FAC_ALG);
	            final PBEKeySpec spec =
	                    new PBEKeySpec(PASSWORD.toCharArray(), saltByte, PSWDITERATIONS, KEY_SIZE);

	            final SecretKey secretKey = factory.generateSecret(spec);
	            final SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), ALGORITHM);

	            // encrypt the message
	            final Cipher cipher = Cipher.getInstance(PADDING);
	            cipher.init(Cipher.ENCRYPT_MODE, secret);

	            final AlgorithmParameters params = cipher.getParameters();
	            ivBytes = params.getParameterSpec(IvParameterSpec.class).getIV();
	            
	            encryptedTextBytes = cipher.doFinal(input.getBytes(CHARATERSET));
	            
	        } catch (NoSuchPaddingException ex) {
	            System.err.println("no cipher in encryptNew");
	        } catch (NoSuchAlgorithmException ex) {
	            System.err.println("no algorithm for factory getInstance in encryptNew");
	        } catch (InvalidKeyException ex) {
	            System.err.println("invalid key for init in encryptNew");
	        } catch (BadPaddingException ex) {
	            System.err.println("bad padding for doFinal in encryptNew");
	        } catch (IllegalBlockSizeException ex) {
	            System.err.println("illegal block for doFinal in encryptNew");
	        } catch (InvalidKeySpecException e) {
	            System.err.println("invalid secret key for generateSecret in encryptNew");
	        } catch (InvalidParameterSpecException e) {
	            System.err.println("invalid parameter for getParameterSpec in encryptNew");
	        } catch (UnsupportedEncodingException e) {
	            System.err.println("unsupported charater set for getBytes in encryptNew");
	        }

	        return new String(Base64.getEncoder().encode(encryptedTextBytes));
	    }
	    
	    /**
	     * new decrypt method with salt and secret key algorithm.
	     *
	     * @param encryptedText the encrypted text.
	     * @return original text.
	     */
	    public static String decrypt(final String encryptedText) {
	        
	        byte[] decryptedTextBytes = null;

	        try {
	            final byte[] saltByte = SALT.getBytes(CHARATERSET);
	            final byte[] encryptedTextBytes =
	                    Base64.getDecoder().decode(encryptedText.getBytes(Charset.forName(CHARATERSET)));

	            // Derive the key
	            final SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_FAC_ALG);
	            final PBEKeySpec spec =
	                    new PBEKeySpec(PASSWORD.toCharArray(), saltByte, PSWDITERATIONS, KEY_SIZE);

	            final SecretKey secretKey = factory.generateSecret(spec);
	            final SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), ALGORITHM);

	            // Decrypt the message
	            final Cipher cipher = Cipher.getInstance(PADDING);
	            cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(ivBytes));
	            
	            decryptedTextBytes = cipher.doFinal(encryptedTextBytes);
	        } catch (NoSuchPaddingException ex) {
	            System.err.println("no cipher in decryptNew");
	        } catch (NoSuchAlgorithmException ex) {
	            System.err.println("no algorithm for factory getInstance in decryptNew");
	        } catch (InvalidKeyException ex) {
	            System.err.println("invalid key for init in decryptNew");
	        } catch (BadPaddingException ex) {
	            System.err.println("bad padding for doFinal in decryptNew");
	        } catch (IllegalBlockSizeException ex) {
	            System.err.println("illegal block for doFinal in decryptNew");
	        } catch (InvalidKeySpecException e) {
	            System.err.println("invalid secret key for generateSecret in decryptNew");
	        } catch (UnsupportedEncodingException e) {
	            System.err.println("unsupported charater set for getBytes in decryptNew");
	        } catch (InvalidAlgorithmParameterException e) {
	            System.err.println("invalid algorithm for init in decryptNew");
	        }

	        return decryptedTextBytes != null ? new String(decryptedTextBytes) : "";
	    }

}
